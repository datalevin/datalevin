(ns datalevin.test.query-or
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing are is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.query.resolve :as qresolve]
   [datalevin.util :as u])
  (:import [clojure.lang ExceptionInfo]
           [java.util List UUID]
           [org.eclipse.collections.impl.list.mutable FastList]))

(use-fixtures :each db-fixture)

(def test-data
  [ {:db/id 1 :name "Ivan" :age 10}
   {:db/id 2 :name "Ivan" :age 20}
   {:db/id 3 :name "Oleg" :age 10}
   {:db/id 4 :name "Oleg" :age 20}
   {:db/id 5 :name "Ivan" :age 10}
   {:db/id 6 :name "Ivan" :age 20} ])

(deftest test-or-join-planning-count-without-materialization
  (let [dir    (u/tmp-dir (str "query-or-count-" (UUID/randomUUID)))
        schema {:knows/from   {:db/valueType :db.type/ref}
                :knows/to     {:db/valueType :db.type/ref}
                :post/creator {:db/valueType :db.type/ref}}
        db     (d/db-with
                 (d/empty-db dir schema)
                 [{:db/id 1 :person/id 1}
                  {:db/id 2 :person/id 2}
                  {:db/id 3 :person/id 3}
                  {:db/id 10 :knows/from 1 :knows/to 2}
                  {:db/id 11 :knows/from 1 :knows/to 3}
                  {:db/id 12 :knows/from 2 :knows/to 3}
                  {:db/id 20 :post/creator 2}
                  {:db/id 21 :post/creator 3}
                  {:db/id 22 :post/creator 3}])
        clause '(or-join [?start ?person]
                         (and [?k :knows/from ?start]
                              [?k :knows/to ?person])
                         (and [?k1 :knows/from ?start]
                              [?k1 :knows/to ?mid]
                              [?k2 :knows/from ?mid]
                              [?k2 :knows/to ?person]))
        tuples (FastList. ^java.util.Collection
                          (mapv object-array [[1] [1]]))
        args   [db {'$ db} nil tuples clause '?start 0 ['?person]
                :post/creator]]
    (try
      (let [counted      (apply qresolve/or-join-count-link args)
            materialized (apply qresolve/or-join-execute-link args)]
        (is (= counted (.size ^List materialized))))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest test-or
  (let [dir     (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        test-db (d/db-with (d/empty-db dir) test-data)]
    (testing "test-or"
      (are [q res] (= (d/q (concat '[:find ?e :where] (quote q)) test-db)
                      (into #{} (map vector) res))

        ;; intersecting results
        [(or [?e :name "Oleg"]
             [?e :age 10])]
        #{1 3 4 5}

        ;; one branch empty
        [(or [?e :name "Oleg"]
             [?e :age 30])]
        #{3 4}

        ;; both empty
        [(or [?e :name "Petr"]
             [?e :age 30])]
        #{}

        ;; join with 1 var
        [[?e :name "Ivan"]
         (or [?e :name "Oleg"]
             [?e :age 10])]
        #{1 5}

        ;; join with 2 vars
        [[?e :age ?a]
         (or (and [?e :name "Ivan"]
                  [1  :age  ?a])
             (and [?e :name "Oleg"]
                  [2  :age  ?a]))]
        #{1 5 4}

        ;; OR introduces vars
        [(or (and [?e :name "Ivan"]
                  [1  :age  ?a])
             (and [?e :name "Oleg"]
                  [2  :age  ?a]))
         [?e :age ?a]]
        #{1 5 4}

        ;; OR introduces vars in different order
        [(or (and [?e :name "Ivan"]
                  [1  :age  ?a])
             (and [2  :age  ?a]
                  [?e :name "Oleg"]))
         [?e :age ?a]]
        #{1 5 4}

        ;; One branch of or short-circuits resolution
        [(or
           (and [?e :age 30] ; no matches in db
                [?e :name ?n])
           (and [?e :age 20]
                [?e :name ?n]))
         [(ground "Ivan") ?n]]
        #{2 6}))

    (testing "test-or-join"
      (are [q res] (= (d/q (concat '[:find ?e :where] (quote q)) test-db)
                      (into #{} (map vector) res))
        [(or-join [?e]
                  [?e :name ?n]
                  (and [?e :age ?a]
                       [?e :name ?n]))]
        #{1 2 3 4 5 6}

        [[?e  :name ?a]
         [?e2 :name ?a]
         (or-join [?e]
                  (and [?e  :age ?a]
                       [?e2 :age ?a]))]
        #{1 2 3 4 5 6}

        [(or-join [?e ?n]
                  (and [?e :age 30] ; no matches in db
                       [?e :name ?n])
                  (and [?e :age 20]
                       [?e :name ?n]))
         [(ground "Ivan") ?n]]
        #{2 6})

      ;; #348
      (is (= #{[1] [3] [4] [5]}
             (d/q '[:find ?e
                    :in $ ?a
                    :where (or
                             [?e :age ?a]
                             [?e :name "Oleg"])]
                  test-db 10)))

      ;; #348
      (is (= #{[1] [3] [4] [5]}
             (d/q '[:find ?e
                    :in $ ?a
                    :where (or-join [?e ?a]
                                    [?e :age ?a]
                                    [?e :name "Oleg"])]
                  test-db 10)))

      ;; #348
      (is (= #{[1] [3] [4] [5]}
             (d/q '[:find ?e
                    :in $ ?a
                    :where (or-join [[?a] ?e]
                                    [?e :age ?a]
                                    [?e :name "Oleg"])]
                  test-db 10)))

      (is (= #{[:a1 :b1 :c1]
               [:a2 :b2 :c2]}
             (d/q '[:find ?a ?b ?c
                    :in $xs $ys
                    :where [$xs ?a ?b ?c] ;; check join by ?a, ignoring ?b, dropping ?c ?d
                    (or-join [?a]
                             [$ys ?a ?b ?d])]
                  [[:a1 :b1 :c1]
                   [:a2 :b2 :c2]
                   [:a3 :b3 :c3]]
                  [[:a1 :b1  :d1] ;; same ?a, same ?b
                   [:a2 :b2* :d2] ;; same ?a, different ?b. Should still be joined
                   [:a4 :b4 :c4]]))) ;; different ?a, should be dropped

      (is (= #{[:a1 :c1] [:a2 :c2]}
             (d/q '[:find ?a ?c
                    :in $xs $ys
                    :where (or-join [?a ?c]
                                    [$xs ?a ?b ?c] ; rel with hole (?b gets dropped, leaving {?a 0 ?c 2} and 3-element tuples)
                                    [$ys ?a ?c])]
                  [[:a1 :b1 :c1]]
                  [[:a2 :c2]]))))

    (testing "test-errors"
      (is (thrown-with-msg? ExceptionInfo #"All clauses in 'or' must use same set of free vars, had \[#\{\?e\} #\{(\?a \?e|\?e \?a)\}\] in \(or \[\?e :name _\] \[\?e :age \?a\]\)"
                            (d/q '[:find ?e
                                   :where (or [?e :name _]
                                              [?e :age ?a])]
                                 test-db)))

      (is (thrown-msg? "Insufficient bindings: #{?e} not bound in (or-join [[?e]] [?e :name \"Ivan\"])"
                       (d/q '[:find ?e
                              :where (or-join [[?e]]
                                              [?e :name "Ivan"])]
                            test-db))))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-default-source
  (let [dir1 (u/tmp-dir (str "test-source-" (UUID/randomUUID)))
        db1  (d/db-with (d/empty-db dir1)
                        [ [:db/add 1 :name "Ivan" ]
                         [:db/add 2 :name "Oleg"] ])
        dir2 (u/tmp-dir (str "test-source-" (UUID/randomUUID)))
        db2  (d/db-with (d/empty-db dir2)
                        [ [:db/add 1 :age 10 ]
                         [:db/add 2 :age 20] ])]
    (are [q res] (= (d/q (concat '[:find ?e :in $ $2 :where] (quote q)) db1 db2)
                    (into #{} (map vector) res))
      ;; OR inherits default source
      [[?e :name]
       (or [?e :name "Ivan"])]
      #{1}

      ;; OR can reference any source
      [[?e :name]
       (or [$2 ?e :age 10])]
      #{1}

      ;; OR can change default source
      [[?e :name]
       ($2 or [?e :age 10])]
      #{1}

      ;; even with another default source, it can reference any other source explicitly
      [[?e :name]
       ($2 or [$ ?e :name "Ivan"])]
      #{1}

      ;; nested OR keeps the default source
      [[?e :name]
       ($2 or (or [?e :age 10]))]
      #{1}

      ;; can override nested OR source
      [[?e :name]
       ($2 or ($ or [?e :name "Ivan"]))]
      #{1})
    (d/close-db db1)
    (d/close-db db2)
    (u/delete-files dir1)
    (u/delete-files dir2)))

(deftest  test-const-substitution
  (let [dir (u/tmp-dir (str "test-const-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:parent {:db/valueType :db.type/ref}})
                (d/db-with [{:db/id "Ivan" :name "Ivan"}
                            {:db/id "Oleg" :name "Oleg" :parent "Ivan"}
                            {:db/id "Petr" :name "Petr" :parent "Oleg"}]))]
    (is (= #{["Ivan" 1 2]}
           (d/q '[:find ?name ?x ?y
                  :in $ ?name
                  :where
                  [?x :name ?name]
                  (or-join [?x ?y]
                           (and
                             [?x :parent ?z]
                             [?z :parent ?y])
                           [?y :parent ?x])]
                db "Ivan")))

    (is (= #{}
           (d/q '[:find ?name ?x ?y
                  :in $ ?name
                  :where
                  [?x :name ?name]
                  (or-join [?x ?y]
                           (and
                             [?x :parent ?z]
                             [?z :parent ?y])
                           [?x :parent ?y])]
                db "Ivan")))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-errors
  (let [dir     (u/tmp-dir (str "test-err-" (UUID/randomUUID)))
        test-db (d/db-with (d/empty-db dir) test-data)]
    (is (thrown-with-msg? ExceptionInfo #"All clauses in 'or' must use same set of free vars, had \[#\{\?e\} #\{(\?a \?e|\?e \?a)\}\] in \(or \[\?e :name _\] \[\?e :age \?a\]\)"
                          (d/q '[:find ?e
                                 :where (or [?e :name _]
                                            [?e :age ?a])]
                               test-db)))

    (is (thrown-msg? "Insufficient bindings: #{?e} not bound in (or-join [[?e]] [?e :name \"Ivan\"])"
                     (d/q '[:find ?e
                            :where (or-join [[?e]]
                                            [?e :name "Ivan"])]
                          test-db)))))
