(ns datalevin.test.query-not
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing are is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.util :as u])
  (:import [clojure.lang ExceptionInfo]
           [java.util UUID]))

(use-fixtures :each db-fixture)

(def test-data
  [ {:db/id 1 :name "Ivan" :age 10}
   {:db/id 2 :name "Ivan" :age 20}
   {:db/id 3 :name "Oleg" :age 10}
   {:db/id 4 :name "Oleg" :age 20}
   {:db/id 5 :name "Ivan" :age 10}
   {:db/id 6 :name "Ivan" :age 20} ])

(deftest test-not
  (let [dir     (u/tmp-dir (str "query-not-test-" (UUID/randomUUID)))
        test-db (d/db-with (d/empty-db dir) test-data)]
    (testing "not"
      (are [q res] (= (set (d/q (concat '[:find [?e ...] :where] (quote q)) test-db))
                      res)
        [[?e :name]
         (not [?e :name "Ivan"])]
        #{3 4}

        [[?e :name]
         (not
           [?e :name "Ivan"]
           [?e :age  10])]
        #{2 3 4 6}

        [[?e :name]
         (not [?e :name "Ivan"])
         (not [?e :age 10])]
        #{4}

        ;; full exclude
        [[?e :name]
         (not [?e :age])]
        #{}

        ;; not-intersecting rels
        [[?e :name "Ivan"]
         (not [?e :name "Oleg"])]
        #{1 2 5 6}

        ;; exclude empty set
        [[?e :name]
         (not [?e :name "Ivan"]
              [?e :name "Oleg"])]
        #{1 2 3 4 5 6}

        ;; nested excludes
        [[?e :name]
         (not [?e :name "Ivan"]
              (not [?e :age 10]))]
        #{1 3 4 5}

        ;; extra binding in not
        [[?e :name ?a]
         (not [?e :age ?f]
              [?e :age 10])]
        #{2 4 6}))

    (testing "test-not-join"
      (are [q res] (= (d/q (concat '[:find ?e ?a :where] (quote q)) test-db)
                      res)
        [[?e :name]
         [?e :age  ?a]
         (not-join [?e]
                   [?e :name "Oleg"]
                   [?e :age ?a])]
        #{[1 10] [2 20] [5 10] [6 20]}

        [[?e :age  ?a]
         [?e :age  10]
         (not-join [?e]
                   [?e :name "Oleg"]
                   [?e :age  ?a]
                   [?e :age  10])]
        #{[1 10] [5 10]}

        [[?e :age ?a]
         (not-join [?a]
                   [?e :name "Petr"]
                   [?e :age ?a])]
        #{[1 10] [2 20] [3 10] [4 20] [5 10] [6 20]}
        ))

    (testing "test-impl-edge-cases"
      (are [q res] (= (d/q (quote q) test-db)
                      res)
        ;; const \ empty
        [:find ?e
         :where [?e :name "Oleg"]
         [?e :age  10]
         (not [?e :age 20])]
        #{[3]}

        ;; const \ const
        [:find ?e
         :where [?e :name "Oleg"]
         [?e :age  10]
         (not [?e :age 10])]
        #{}

        ;; rel \ const
        [:find ?e
         :where [?e :name "Oleg"]
         (not [?e :age 10])]
        #{[4]}

        ;; 2 rels \ 2 rels
        [:find ?e ?e2
         :where [?e  :name "Ivan"]
         [?e2 :name "Ivan"]
         (not [?e :age 10]
              [?e2 :age 20])]
        #{[2 1] [6 5] [1 1] [2 2] [5 5] [6 6] [2 5] [1 5] [2 6] [6 1] [5 1] [6 2]}

        ;; 2 rels \ rel + const
        [:find ?e ?e2
         :where [?e  :name "Ivan"]
         [?e2 :name "Oleg"]
         (not [?e :age 10]
              [?e2 :age 20])]
        #{[2 3] [1 3] [2 4] [6 3] [5 3] [6 4]}

        ;; 2 rels \ 2 consts
        [:find ?e ?e2
         :where [?e  :name "Oleg"]
         [?e2 :name "Oleg"]
         (not [?e :age 10]
              [?e2 :age 20])]
        #{[4 3] [3 3] [4 4]}
        ))
    (testing "test-insufficient-bindings"
      (are [q msg] (thrown-msg? msg
                                (d/q (concat '[:find ?e :where] (quote q)) test-db))

        [[?e :name]
         (not-join [?e]
                   (not [1 :age ?a])
                   [?e :age ?a])]
        "Insufficient bindings: none of #{?a} is bound in (not [1 :age ?a])"

        [[?e :name]
         (not [?a :name "Ivan"])]
        "Insufficient bindings: none of #{?a} is bound in (not [?a :name \"Ivan\"])"
        ))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-default-source
  (let [dir1 (u/tmp-dir (str "or-test-" (UUID/randomUUID)))
        db1  (d/db-with (d/empty-db dir1)
                        [ [:db/add 1 :name "Ivan" ]
                         [:db/add 2 :name "Oleg"] ])
        dir2 (u/tmp-dir (str "or-test-" (UUID/randomUUID)))
        db2  (d/db-with (d/empty-db dir2)
                        [ [:db/add 1 :age 10 ]
                         [:db/add 2 :age 20] ])]
    (are [q res] (= (set (d/q (concat '[:find [?e ...]
                                        :in   $ $2
                                        :where]
                                      (quote q))
                              db1 db2))
                    res)
      ;; NOT inherits default source
      [[?e :name]
       (not [?e :name "Ivan"])]
      #{2}

      ;; NOT can reference any source
      [[?e :name]
       (not [$2 ?e :age 10])]
      #{2}

      ;; NOT can change default source
      [[?e :name]
       ($2 not [?e :age 10])]
      #{2}

      ;; even with another default source, it can reference any other source explicitly
      [[?e :name]
       ($2 not [$ ?e :name "Ivan"])]
      #{2}

      ;; nested NOT keeps the default source
      [[?e :name]
       ($2 not (not [?e :age 10]))]
      #{1}

      ;; can override nested NOT source
      [[?e :name]
       ($2 not ($ not [?e :name "Ivan"]))]
      #{1})
    (d/close-db db1)
    (d/close-db db2)
    (u/delete-files dir1)
    (u/delete-files dir2)))

(deftest test-not-join-planner-coverage
  (let [dir     (u/tmp-dir (str "query-not-plan-" (UUID/randomUUID)))
        test-db (d/db-with (d/empty-db dir) test-data)]
    (testing "simple not-join gets planned"
      (let [q   '[:find ?e
                  :where [?e :name]
                  (not-join [?e]
                            [?e :name "Ivan"])]
            exp (d/explain {} q test-db)
            steps
            (->> exp
                 :plan
                 vals
                 (mapcat identity)
                 (mapcat identity)
                 (mapcat :steps)
                 vec)]
        (is (= #{[3] [4]} (d/q q test-db)))
        (is (some #(re-find #"Anti-join by" %) steps))
        (is (not-any? #(= '(not-join [?e] [?e :name "Ivan"]) %)
                      (:late-clauses exp)))))

    (testing "complex not-join still falls back to late-clauses"
      (let [q   '[:find ?e
                  :where [?e :name]
                  (not-join [?e]
                            (not [?e :age 10]))]
            exp (d/explain {} q test-db)]
        (is (= #{[1] [3] [5]} (d/q q test-db)))
        (is (some #(= '(not-join [?e] (not [?e :age 10])) %)
                  (:late-clauses exp)))))

    (d/close-db test-db)
    (u/delete-files dir)))
