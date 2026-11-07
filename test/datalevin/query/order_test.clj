(ns datalevin.query.order-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query :as query]
            [datalevin.query.execute.result :as result]
            [datalevin.query.source.fake :as fake]
            [datalevin.test.core :refer [db-fixture]]))

(use-fixtures :each db-fixture)

(def ^:private people
  [{:db/id 1 :person/name "Zoe"}
   {:db/id 2 :person/name "Amy"}
   {:db/id 3 :person/name "Bob"}
   {:db/id 4 :person/name "Amy"}])

(defn- check-query [db q expected & inputs]
  (let [prepared (d/prepare-q db q)]
    (dotimes [_ 2]
      (is (= expected (apply d/q q db inputs)))
      (is (= expected (prepared (vec inputs)))))
    (binding [query/*cache?* false]
      (is (= expected (apply d/q q db inputs))))))

(deftest collection-ordering-precedes-projection
  (with-open [conn (d/create-conn nil {:person/name {:db/valueType :db.type/string}}
                                 {:background-sampling? false})]
    (d/transact! conn people)
    (doseq [direction [:asc :desc]
            column ['?eid 0]
            pulled? [false true]
            :let [ordered (if (= direction :asc) people (vec (reverse people)))
                  q {:find (if pulled? '[[(pull ?eid [:person/name]) ...]]
                               '[[?eid ...]])
                     :where '[[?eid :person/name ?name]]
                     :order-by [[column direction]]}
                  expected (mapv (if pulled? #(dissoc % :db/id) :db/id) ordered)]]
      (check-query @conn q expected)
      (check-query @conn (assoc q :in '[$ ?limit ?offset]
                                 :limit '[?limit] :offset '[?offset])
                   (subvec expected 1 3) 2 1)
      (check-query @conn (assoc q :limit [-1]) expected)
      (check-query @conn (assoc q :offset [10]) []))))

(deftest source-collection-ordering-precedes-projection
  (let [source (fake/source (fake/info {:person/name {:entity-type :long
                                                    :value-type :string}})
                            (mapv #(vector (:db/id %) :person/name (:person/name %)) people))
        q '{:find [[?eid ...]] :in [$s]
            :where [[$s ?eid :person/name ?name]] :order-by [[?eid :desc]]}]
    (is (= [4 3 2 1] (d/q q source)))
    (is (= [3 2] (d/q (assoc q :limit 2 :offset 1) source)))))

(deftest ordered-return-maps-retain-query-key-equality
  (let [return-map (parser/parse-return-map :keys '[value])
        rows [[1.0M] [1.00M] [1.0M] [2.0M]]]
    (doseq [options [nil {:spill-threshold -1}]
            tuples [rows (mapv object-array rows)]]
      (is (= ["1.0" "1.00" "2.0"]
             (mapv (comp str :value)
                   (result/tuples->return-map return-map tuples options true)))))))

(deftest pull-ordering-uses-entity-ids
  (with-open [conn (d/create-conn nil {:person/name {:db/valueType :db.type/string}}
                                 {:background-sampling? false})]
    (d/transact! conn people)
    (doseq [direction [:asc :desc]
            column ['?eid 0]
            :let [ordered (if (= direction :asc) people (vec (reverse people)))
                  q {:find '[(pull ?eid [:person/name]) ?name]
                     :where '[[?eid :person/name ?name]]
                     :order-by [[column direction]]}
                  expected (mapv #(vector (dissoc % :db/id) (:person/name %)) ordered)]]
      (check-query @conn q expected)
      (check-query @conn (assoc q :limit [2] :offset [1]) (subvec expected 1 3)))))

(deftest return-maps-preserve-order-and-distinctness
  (with-open [conn (d/create-conn nil {:person/name {:db/valueType :db.type/string}}
                                 {:background-sampling? false})]
    (d/transact! conn people)
    (doseq [[return-type labels] [[:keys [:person-id :person-name]]
                                 [:syms '[person-id person-name]]
                                 [:strs ["person-id" "person-name"]]]
            pulled? [false true]
            ordering ['[?name :asc ?eid :desc] '[1 :asc 0 :desc]]
            :let [q {:find (if pulled? '[(pull ?eid [:person/name]) ?name]
                              '[?eid ?name])
                     return-type '[person-id person-name]
                     :where '[[?eid :person/name ?name]]
                     :order-by [ordering]}
                  expected (mapv #(zipmap labels [(if pulled? (dissoc % :db/id) (:db/id %))
                                                  (:person/name %)])
                                 (mapv people [3 1 2 0]))
                  expected (if pulled? (vec (distinct expected)) expected)]]
      (check-query @conn q expected)
      (check-query @conn (assoc q :limit [2] :offset [1]) (subvec expected 1 3)))
    (testing "ordering a pulled entity column also works with return maps"
      (check-query @conn
                   '[:find (pull ?eid [:person/name])
                     :keys person
                     :where [?eid :person/name ?name]
                     :order-by [?eid :asc]
                     :offset 1 :limit 2]
                   [{:person {:person/name "Amy"}}
                    {:person {:person/name "Bob"}}]))))

(deftest pull-can-order-by-a-separate-find-column
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #":order-by variable that is not in :find spec"
        (parser/parse-query
          '[:find [(pull ?eid [:db/id :person/name]) ...]
            :where [?eid :person/name ?name]
            :order-by [?name :asc]])))
  (with-open [conn (d/create-conn nil {:person/name {:db/valueType :db.type/string}}
                                 {:background-sampling? false})]
    (d/transact! conn people)
    (let [q '[:find (pull ?eid [:db/id :person/name]) ?name
              :where [?eid :person/name ?name]
              :order-by [?name :asc ?eid :asc]]]
      (check-query @conn q
                   (mapv #(vector % (:person/name %)) (mapv people [1 3 2 0]))))))
