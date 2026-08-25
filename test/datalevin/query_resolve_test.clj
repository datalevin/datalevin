;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice.
;;

(ns datalevin.query-resolve-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.core :as d]
   [datalevin.query.execute]
   [datalevin.query.plan :as qplan]
   [datalevin.rules :as rules]))

(deftest flat-function-tuple-binding-test
  (testing "ignored elements and new variables bind directly"
    (is (= #{[1 "one"] [2 "two"]}
           (d/q '[:find ?id ?value
                  :in [[?id ?label]]
                  :where
                  [(vector :ignored ?label) [_ ?value]]]
                [[1 "one"] [2 "two"]]))))

  (testing "tuple outputs still unify with variables in the production row"
    (is (= #{[1]}
           (d/q '[:find ?id
                  :in [[?id ?expected]]
                  :where
                  [(vector :ignored ?id) [_ ?expected]]]
                [[1 1] [2 9]]))))

  (testing "all-ignored tuples retain matching production rows"
    (is (= #{[1] [2]}
           (d/q '[:find ?id
                  :in [?id ...]
                  :where
                  [(vector ?id :ignored) [_ _]]]
                [1 2]))))

  (testing "nil tuple results continue to filter production rows"
    (is (= #{[1 10] [3 30]}
           (d/q '[:find ?id ?value
                  :in [?id ...] ?tuple-fn
                  :where
                  [(?tuple-fn ?id) [_ ?value]]]
                [1 2 3]
                (fn [^long id]
                  (when (odd? id) [:ignored (* id 10)])))))))

(deftest nested-rule-set-boundary-test
  (let [domain (range 12)
        facts  (vec
                 (for [attr [:d1 :d2 :c2 :c3 :c4]
                       x domain
                       y domain]
                   [x attr y]))
        rules  '[[(c1 ?x ?y)
                  [?x :d1 ?z]
                  [?z :d2 ?y]]
                 [(b2 ?x ?y)
                  [?x :c3 ?z]
                  [?z :c4 ?y]]
                 [(b1 ?x ?y)
                  (c1 ?x ?z)
                  [?z :c2 ?y]]
                 [(a ?x ?y)
                  (b1 ?x ?z)
                  (b2 ?z ?y)]]
        ff-q   '[:find ?x ?y :in $ % :where (a ?x ?y)]
        b1-q   '[:find ?x ?y :in $ % :where (b1 ?x ?y)]
        bf-q   '[:find ?y :in $ % ?x :where (a ?x ?y)]
        fb-q   '[:find ?x :in $ % ?y :where (a ?x ?y)]]
    (testing "nested predicates collapse duplicate proofs at rule boundaries"
      (is (= (* (count domain) (count domain))
             (count (d/q ff-q facts rules))))
      (is (= (count domain) (count (d/q bf-q facts rules 1))))
      (is (= (count domain) (count (d/q fb-q facts rules 1)))))

    (testing "a derived-relation composition remains a planned rule boundary"
      (is (= 'a (ffirst (:late-clauses
                          (d/explain {:run? false} ff-q facts rules)))))
      (is (= 'b1 (ffirst (:late-clauses
                           (d/explain {:run? false} b1-q facts rules))))))))

(deftest terminal-projected-distinct-collection-test
  (let [schema {:c3 {:db/valueType   :db.type/ref
                     :db/cardinality :db.cardinality/many}
                :c4 {:db/valueType   :db.type/ref
                     :db/cardinality :db.cardinality/many}}
        conn   (d/create-conn nil schema {:kv-opts {:inmemory? true}})
        rules  '[[(b2 ?x ?y)
                  [?x :c3 ?z]
                  [?z :c4 ?y]]]
        query  '[:find ?x ?y :in $ % :where (b2 ?x ?y)]]
    (try
      (d/transact! conn [{:db/id 1 :c3 [10 11]}
                         {:db/id 10 :c4 [20 21]}
                         {:db/id 11 :c4 [20]}])
      (let [explain (d/explain {:run? true} query (d/db conn) rules)]
        (is (= #{[1 20] [1 21]} (:result explain)))
        (is (= {:mode          :projected-distinct
                :symbols       '[?x ?y]
                :input-tuples  3
                :result-tuples 2}
               (:terminal-result-collection explain))))
      (finally
        (d/close conn)))))

(deftest terminal-eav-binary-composition-test
  (let [n      64
        domain (vec (range 1 (inc n)))
        schema {:c3 {:db/valueType   :db.type/ref
                     :db/cardinality :db.cardinality/many}
                :c4 {:db/valueType   :db.type/ref
                     :db/cardinality :db.cardinality/many}}
        conn   (d/create-conn nil schema {:kv-opts {:inmemory? true}})
        rules  '[[(b2 ?x ?y)
                  [?x :c3 ?z]
                  [?z :c4 ?y]]]
        query  '[:find ?x ?y :in $ % :where (b2 ?x ?y)]]
    (try
      (d/transact! conn
                   (mapv (fn [x]
                           {:db/id x, :c3 domain, :c4 domain})
                         domain))
      (let [database (d/db conn)
            explain  (d/explain {:run? true} query database rules)
            terminal (:terminal-result-collection explain)
            physical (:physical-operator terminal)
            expected (set (for [x domain, y domain] [x y]))]
        (is (= expected (:result explain)))
        (is (= (* n n n) (:input-tuples terminal)))
        (is (= (* n n) (:result-tuples terminal)))
        (is (= :dense-eav-composition (:operator physical)))
        (is (= :full-ave (:target-scan physical)))
        (is (= :dense (:bitmap physical)))
        (is (= (* n n n) (:candidate-pairs physical)))
        (is (= expected
               (binding [qplan/*terminal-eav-composition?* false]
                 (d/q query database rules)))))
      (finally
        (d/close conn)))))

(deftest terminal-eav-composition-distinct-bound-scan-test
  (let [join-keys (vec (range 1 17))
        values    (vec (range 100 164))
        anchors   (vec (range 1000 1064))
        unrelated (vec (range 2000 2080))
        schema    {:c3 {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}
                   :c4 {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}}
        conn      (d/create-conn nil schema {:kv-opts {:inmemory? true}})
        rules     '[[(b2 ?x ?y)
                     [?x :c3 ?z]
                     [?z :c4 ?y]]]
        query     '[:find ?x ?y :in $ % :where (b2 ?x ?y)]]
    (try
      (d/transact!
        conn
        (vec (concat
               (map (fn [x] {:db/id x, :c3 join-keys}) anchors)
               (map (fn [z] {:db/id z, :c4 values}) join-keys)
               (map (fn [z] {:db/id z, :c4 values}) unrelated))))
      (let [explain  (d/explain {:run? true} query (d/db conn) rules)
            terminal (:terminal-result-collection explain)
            physical (:physical-operator terminal)]
        (is (= (set (for [x anchors, y values] [x y]))
               (:result explain)))
        (is (= :dense-eav-composition (:operator physical)))
        (is (= :distinct-bound-eav (:target-scan physical)))
        (is (= (count join-keys) (:distinct-join-keys physical)))
        (is (= (* (+ (count join-keys) (count unrelated))
                  (count values))
               (:target-attribute-tuples physical)))
        (is (= (* (count join-keys) (count values))
               (:target-tuples physical))))
      (finally
        (d/close conn)))))

(deftest bound-late-rule-order-test
  (let [sort-late @(ns-resolve 'datalevin.query.execute 'sort-late-clauses)
        rules     {'left :defined, 'right :defined}
        clauses   '[(left ?x ?join) (right ?join 1)]]
    (testing "a ready rule with a constant argument starts the rule DAG"
      (is (= '[(right ?join 1) (left ?x ?join)]
             (sort-late #{} rules clauses))))
    (testing "ties retain source order"
      (is (= clauses (sort-late #{'?x} rules clauses))))))

(deftest bound-transitive-eav-specialization-test
  (let [schema       {:edge {:db/valueType   :db.type/ref
                             :db/cardinality :db.cardinality/many}}
        facts        [{:db/id 1 :db/ident :node/one :edge [2]}
                      {:db/id 2 :edge [3 4]}
                      {:db/id 3 :edge [1]}
                      {:db/id 4}]
        conn         (d/create-conn nil schema
                                    {:kv-opts {:inmemory? true}})
        fallback     (d/create-conn nil schema
                                    {:kv-opts {:inmemory? true}})
        left-rules   '[[(tc ?a ?b)
                        [?a :edge ?b]]
                       [(tc ?a ?b)
                        [?a :edge ?mid]
                        (tc ?mid ?b)]]
        right-rules  '[[(tc-right ?a ?b)
                        [?a :edge ?b]]
                       [(tc-right ?a ?b)
                        (tc-right ?a ?mid)
                        [?mid :edge ?b]]]
        reverse-rules '[[(tc-reverse ?a ?b)
                         [?b :edge ?a]]
                        [(tc-reverse ?a ?b)
                         [?mid :edge ?a]
                         (tc-reverse ?mid ?b)]]
        constrained-rules
        '[[(tc-constrained ?a ?b)
           [?a :edge ?b]]
          [(tc-constrained ?a ?b)
           [?a :edge ?a]
           (tc-constrained ?a ?b)]]
        bf-query     '[:find [?b ...]
                       :in $ % ?start
                       :where (tc ?start ?b)]
        fb-query     '[:find [?a ...]
                       :in $ % ?end
                       :where (tc ?a ?end)]]
    (try
      (d/transact! conn facts)
      (d/transact! fallback facts)
      (testing "a singleton input traverses only the indexed reachable graph"
        (is (= #{1 2 3 4}
               (set (d/q bf-query (d/db conn) left-rules 1))))
        (is (= #{1 2 3}
               (set (d/q fb-query (d/db conn) left-rules 4))))
        (is (= #{1 2 3 4}
               (set (d/q '[:find [?b ...]
                           :in $ %
                           :where (tc 1 ?b)]
                         (d/db conn) left-rules))))
        (is (= #{1 2 3 4}
               (set (d/q bf-query (d/db conn) left-rules :node/one)))))

      (testing "right-linear and physically reversed forms retain semantics"
        (is (= #{1 2 3 4}
               (set (d/q '[:find [?b ...]
                           :in $ % ?start
                           :where (tc-right ?start ?b)]
                         (d/db conn) right-rules 1))))
        (is (= #{1 2 3}
               (set (d/q '[:find [?b ...]
                           :in $ % ?start
                           :where (tc-reverse ?start ?b)]
                         (d/db conn) reverse-rules 4))))
        (is (= #{1 2 3 4}
               (set (d/q '[:find [?a ...]
                           :in $ % ?end
                           :where (tc-reverse ?a ?end)]
                         (d/db conn) reverse-rules 1)))))

      (testing "variable equality constraints stay on the general evaluator"
        (is (= #{2}
               (set (d/q '[:find [?b ...]
                           :in $ % ?start
                           :where (tc-constrained ?start ?b)]
                         (d/db conn) constrained-rules 1)))))

      (testing "the specialized result agrees with the general fixed point"
        (is (= (set (d/q bf-query (d/db conn) left-rules 1))
               (binding [rules/*bound-transitive-eav?* false]
                 (set (d/q bf-query (d/db fallback) left-rules 1)))))
        (is (= (set (d/q fb-query (d/db conn) left-rules 4))
               (binding [rules/*bound-transitive-eav?* false]
                 (set (d/q fb-query (d/db fallback) left-rules 4))))))

      (testing "a pending transaction overlay uses transaction-aware probes"
        (let [overlay (d/db-with (d/db conn) [[:db/add 4 :edge 5]])]
          (is (= #{1 2 3 4 5}
                 (set (d/q '[:find [?b ...]
                             :in $ % ?start
                             :where (tc-overlay ?start ?b)]
                           overlay
                           '[[(tc-overlay ?a ?b)
                              [?a :edge ?b]]
                             [(tc-overlay ?a ?b)
                              [?a :edge ?mid]
                              (tc-overlay ?mid ?b)]]
                           1))))))
      (finally
        (d/close conn)
        (d/close fallback)))))
