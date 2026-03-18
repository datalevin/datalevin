(ns datalevin.test.query
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.util :as u])
  (:import
   [clojure.lang ExceptionInfo]
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest test-instant
  (let [dir (u/tmp-dir (str "test-instant-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir
                            {:person/born {:db/valueType :db.type/instant}})
                (d/db-with [{:person/born #inst "1969-01-01"}
                            {:person/born #inst "1971-01-01"}]))]
    (is (= 2 (count (d/datoms db :eav))))
    (is (= 2 (count
               (d/q '[:find [?born ...]
                      :where [?e :person/born ?born]] db))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-many-joins
  (let [data (->> (range 1000)
                  (map (fn [^long i]
                         {:db/id i
                          :a     (str (UUID/randomUUID))
                          :b     (str (UUID/randomUUID))
                          :c     (str (UUID/randomUUID))
                          :d     (str (UUID/randomUUID))
                          :e     (rand-int 3)
                          :f     (rand-int 3)
                          :g     (rand-int 3)
                          :h     (rand-int 3)})))
        dir  (u/tmp-dir (str "test-many-" (UUID/randomUUID)))
        db   (-> (d/empty-db dir {:a {:db/valueType :db.type/string}
                                  :b {:db/valueType :db.type/string}
                                  :c {:db/valueType :db.type/string}
                                  :d {:db/valueType :db.type/string}
                                  :e {:db/valueType :db.type/long}
                                  :f {:db/valueType :db.type/long}
                                  :g {:db/valueType :db.type/long}
                                  :h {:db/valueType :db.type/long}})
                 (d/db-with data))]
    (is (number? (d/q '[:find ?eid1 .
                        :where
                        [?eid1 :a ?a1]
                        [?eid1 :b ?b1]
                        [?eid1 :c ?c1]
                        [?eid1 :d ?d1]
                        [?eid1 :e ?e1]
                        [?eid1 :f ?f1]
                        [?eid1 :g ?g1]
                        [?eid1 :h ?h1]
                        [?eid2 :e ?e1]]
                      db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-joins
  (let [dir (u/tmp-dir (str "test-instant-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir)
                (d/db-with [ { :db/id 1, :name "Ivan", :age 15 }
                            { :db/id 2, :name "Petr", :age 37 }
                            { :db/id 3, :name "Ivan", :age 37 }
                            { :db/id 4, :age 15 }]))]

    (is (= (d/q '[:find ?e
                  :where [?e :name]] db)
           #{[1] [2] [3]}))
    (is (= (d/q '[:find  ?e ?v
                  :where [?e :name "Ivan"]
                  [?e :age ?v]] db)
           #{[1 15] [3 37]}))
    (is (= (d/q '[:find  ?e1 ?e2
                  :where [?e1 :name ?n]
                  [?e2 :name ?n]] db)
           #{[1 1] [2 2] [3 3] [1 3] [3 1]}))
    (is (= (d/q '[:find  ?e ?e2 ?n
                  :where [?e :name "Ivan"]
                  [?e :age ?a]
                  [?e2 :age ?a]
                  [?e2 :name ?n]] db)
           #{[1 1 "Ivan"]
             [3 3 "Ivan"]
             [3 2 "Petr"]}))
    (d/close-db db)
    (u/delete-files dir)))


(deftest test-q-many
  (let [dir (u/tmp-dir (str "test-query-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:aka {:db/cardinality :db.cardinality/many}})
                (d/db-with [ [:db/add 1 :name "Ivan"]
                            [:db/add 1 :aka  "ivolga"]
                            [:db/add 1 :aka  "pi"]
                            [:db/add 2 :name "Petr"]
                            [:db/add 2 :aka  "porosenok"]
                            [:db/add 2 :aka  "pi"] ]))]
    (is (= (d/q '[:find  ?n1 ?n2
                  :where [?e1 :aka ?x]
                  [?e2 :aka ?x]
                  [?e1 :name ?n1]
                  [?e2 :name ?n2]] db)
           #{["Ivan" "Ivan"]
             ["Petr" "Petr"]
             ["Ivan" "Petr"]
             ["Petr" "Ivan"]}))
    (d/close-db db)
    (u/delete-files dir)))


(deftest test-q-coll
  (let [db [[1 :name "Ivan"]
            [1 :age  19]
            [1 :aka  "dragon_killer_94"]
            [1 :aka  "-=autobot=-"] ] ]
    (is (= (d/q '[ :find  ?n ?a
                  :where [?e :aka "dragon_killer_94"]
                  [?e :name ?n]
                  [?e :age  ?a]] db)
           #{["Ivan" 19]})))

  (testing "Query over long tuples"
    (let [db [ [1 :name "Ivan" 945 :db/add]
              [1 :age  39     999 :db/retract]] ]
      (is (= (d/q '[ :find  ?e ?v
                    :where [?e :name ?v]] db)
             #{[1 "Ivan"]}))
      (is (= (d/q '[ :find  ?e ?a ?v ?t
                    :where [?e ?a ?v ?t :db/retract]] db)
             #{[1 :age 39 999]})))))


(deftest test-q-in
  (let [dir   (u/tmp-dir (str "test-q-in-" (UUID/randomUUID)))
        db    (-> (d/empty-db dir)
                  (d/db-with [ { :db/id 1, :name "Ivan", :age 15 }
                              { :db/id 2, :name "Petr", :age 37 }
                              { :db/id 3, :name "Ivan", :age 37 }]))
        query '{:find  [?e]
                :in    [$ ?attr ?value]
                :where [[?e ?attr ?value]]}]
    (is (= (d/q query db :name "Ivan")
           #{[1] [3]}))
    (is (= (d/q query db :age 37)
           #{[2] [3]}))

    (testing "Named DB"
      (is (= (d/q '[:find  ?a ?v
                    :in    $db ?e
                    :where [$db ?e ?a ?v]] db 1)
             #{[:name "Ivan"]
               [:age 15]})))

    (testing "DB join with collection"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ $b
                    :where [?e :name ?n]
                    [$b ?n ?email]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))

    (testing "Query without DB"
      (is (= (d/q '[:find ?a ?b
                    :in   ?a ?b]
                  10 20)
             #{[10 20]})))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-bindings
  (let [dir (u/tmp-dir (str "test-instant-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir)
                (d/db-with [ { :db/id 1, :name "Ivan", :age 15 }
                            { :db/id 2, :name "Petr", :age 37 }
                            { :db/id 3, :name "Ivan", :age 37 }]))]
    (testing "Relation binding"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ [[?n ?email]]
                    :where [?e :name ?n]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))

    (testing "Tuple binding"
      (is (= (d/q '[:find  ?e
                    :in    $ [?name ?age]
                    :where [?e :name ?name]
                    [?e :age ?age]]
                  db ["Ivan" 37])
             #{[3]})))

    (testing "Collection binding"
      (is (= (d/q '[:find  ?attr ?value
                    :in    $ ?e [?attr ...]
                    :where [?e ?attr ?value]]
                  db 1 [:name :age])
             #{[:name "Ivan"] [:age 15]})))

    (testing "Empty coll handling"
      (is (= (d/q '[:find ?id
                    :in $ [?id ...]
                    :where [?id :age _]]
                  [[1 :name "Ivan"]
                   [2 :name "Petr"]]
                  [])
             #{}))
      (is (= (d/q '[:find ?id
                    :in $ [[?id]]
                    :where [?id :age _]]
                  [[1 :name "Ivan"]
                   [2 :name "Petr"]]
                  [])
             #{})))

    (testing "Placeholders"
      (is (= (d/q '[:find ?x ?z
                    :in [?x _ ?z]]
                  [:x :y :z])
             #{[:x :z]}))
      (is (= (d/q '[:find ?x ?z
                    :in [[?x _ ?z]]]
                  [[:x :y :z] [:a :b :c]])
             #{[:x :z] [:a :c]})))

    (testing "Error reporting"
      (is (thrown-with-msg? ExceptionInfo #"Cannot bind value :a to tuple \[\?a \?b\]"
                            (d/q '[:find ?a ?b :in [?a ?b]] :a)))
      (is (thrown-with-msg? ExceptionInfo #"Cannot bind value :a to collection \[\?a \.\.\.\]"
                            (d/q '[:find ?a :in [?a ...]] :a)))
      (is (thrown-with-msg? ExceptionInfo #"Not enough elements in a collection \[:a\] to bind tuple \[\?a \?b\]"
                            (d/q '[:find ?a ?b :in [?a ?b]] [:a]))))

    (d/close-db db)
    (u/delete-files dir)))

(deftest test-nested-bindings
  (is (= (d/q '[:find  ?k ?v
                :in    [[?k ?v] ...]
                :where [(> ?v 1)]]
              {:a 1, :b 2, :c 3})
         #{[:b 2] [:c 3]}))

  (is (= (d/q '[:find  ?k ?min ?max
                :in    [[?k ?v] ...] ?minmax
                :where [(?minmax ?v) [?min ?max]]
                [(> ?max ?min)]]
              {:a [1 2 3 4]
               :b [5 6 7]
               :c [3]}
              (fn [v] [(reduce min v) (reduce max v)]))
         #{[:a 1 4] [:b 5 7]}))

  (is (= (d/q '[:find  ?k ?x
                :in    [[?k [?min ?max]] ...] ?range
                :where [(?range ?min ?max) [?x ...]]
                [(even? ?x)]]
              {:a [1 7]
               :b [2 4]}
              range)
         #{[:a 2] [:a 4] [:a 6]
           [:b 2]})))

(deftest test-built-in-regex
  (is (= (d/q '[:find  ?name
                :in    [?name ...] ?key
                :where [(re-pattern ?key) ?pattern]
                [(re-find ?pattern ?name)]]
              #{"abc" "abcX" "aXb"}
              "X")
         #{["abcX"] ["aXb"]})))

(deftest test-some-strings
  (let [dir  (u/tmp-dir (str "test-instant-" (UUID/randomUUID)))
        conn (d/create-conn dir {:id   {:db/valueType :db.type/long}
                                 :text {:db/valueType :db.type/string}})]
    (d/transact! conn [{:text "[7/3, 15:36]"
                        :id   3}])
    (is (= '([{:db/id 1, :id 3, :text "[7/3, 15:36]"}])
           (d/q '[:find (pull ?e [*])
                  :where
                  [?e :id 3]]
                @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-issue-359-vector-value-passed-to-udf
  (let [dir  (u/tmp-dir (str "test-issue-359-" (UUID/randomUUID)))
        conn (d/get-conn dir)
        seen (atom ::unset)
        pred (fn [coord]
               (reset! seen coord)
               (= coord [1 2 4]))]
    (try
      (d/transact! conn [{:db/id -1 :at/coord [1 2 4]}])
      (is (= [{:db/id 1 :at/coord [1 2 4]}]
             (d/q '[:find [(pull ?e [*]) ...]
                    :in $ ?coord-in-range
                    :where
                    [?e :at/coord ?coord]
                    [(?coord-in-range ?coord)]]
                  @conn
                  pred)))
      (is (= [1 2 4] @seen))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-built-in-get
  (is (= (d/q '[:find ?m ?m-value
                :in [[?k ?m] ...] ?m-key
                :where [(get ?m ?m-key) ?m-value]]
              {:a {:b 1}
               :c {:d 2}}
              :d)
         #{[{:d 2} 2]})))

(deftest test-join-unrelated
  (let [dir (u/tmp-dir (str "test-query-" (UUID/randomUUID)))
        db  (d/empty-db dir)]
    (is (= #{}
           (d/q '[:find ?name
                  :in $ ?my-fn
                  :where [?e :person/name ?name]
                  [(?my-fn) ?result]
                  [(< ?result 3)]]
                (d/db-with db [{:person/name "Joe"}])
                (fn [] 5))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-symbol-comparison
  (is (= [2]
         (d/q
           '[:find [?e ...]
             :where [?e :s b]]
           '[[1 :s a]
             [2 :s b]])))
  (let [db (-> (d/empty-db)
               (d/db-with '[{:db/id 1, :s a}
                            {:db/id 2, :s b}]))]
    (is (= [2]
           (d/q
             '[:find [?e ...]
               :where [?e :s b]]
             db)))))
