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
   [datalevin.query.execute]))

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

(deftest bound-late-rule-order-test
  (let [sort-late @(ns-resolve 'datalevin.query.execute 'sort-late-clauses)
        rules     {'left :defined, 'right :defined}
        clauses   '[(left ?x ?join) (right ?join 1)]]
    (testing "a ready rule with a constant argument starts the rule DAG"
      (is (= '[(right ?join 1) (left ?x ?join)]
             (sort-late #{} rules clauses))))
    (testing "ties retain source order"
      (is (= clauses (sort-late #{'?x} rules clauses))))))
