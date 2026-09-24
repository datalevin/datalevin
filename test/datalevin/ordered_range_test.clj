(ns datalevin.ordered-range-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.query :as q]
            [datalevin.query.execute :as execute]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(defn- with-records [f]
  (let [dir (u/tmp-dir (str "ordered-range-" (random-uuid)))
        conn (d/get-conn dir
                        {:rank {:db/valueType :db.type/long}
                         :name {:db/valueType :db.type/string}
                         :keep {:db/valueType :db.type/boolean}}
                        {:cache-limit 0})]
    (try
      (d/transact! conn
                   (mapv (fn [n]
                           {:db/id (+ 1 (* 3 n)) :rank (* 2 n)
                            :name "same" :keep (zero? (mod n 3))})
                         (range 2500)))
      (binding [q/*cache?* false]
        (f conn (db/-clear-tx-cache @conn)))
      (finally (d/close conn) (u/delete-files dir)))))

(defn- conventional [query & inputs]
  (binding [execute/*access-methods* []]
    (apply d/q query inputs)))

(deftest ordered-range-page-test
  (with-records
    (fn [_ database]
      (doseq [[predicate direction]
              [['(>= ?rank ?start) :asc] ['(> ?rank ?start) :asc]
               ['(<= ?start ?rank) :asc] ['(< ?start ?rank) :asc]
               ['(<= ?rank ?start) :desc] ['(< ?rank ?start) :desc]
               ['(>= ?start ?rank) :desc] ['(> ?start ?rank) :desc]]
              offset [0 3]]
        (testing (str predicate " " direction " offset=" offset)
          (let [query {:find '[?e ?rank]
                       :in '[$ ?start]
                       :where [['?e :rank '?rank] [predicate]]
                       :order-by [['?rank direction]] :limit 5 :offset offset}
                explain (d/explain {:run? true} query database 2000)]
            (is (= (conventional query database 2000)
                   (d/q query database 2000)))
            (is (true? (:access-path-selected? explain)))
            (is (= :ave (get-in explain [:plan :access-plan :method])))
            (is (<= (get-in explain [:plan :candidate-count])
                    (* 2 (+ offset 5))))
            (is (nil? (get-in explain [:plan :fallback])))))))))

(deftest ordered-range-pulls-only-selected-page-test
  (with-records
    (fn [_ database]
      (let [pulled (atom 0)
            pattern [[:name :xform (fn [value] (swap! pulled inc) value)]]
            query '[:find ?rank (pull ?e ?pattern)
                    :in $ ?start ?pattern
                    :where [?e :rank ?rank] [(>= ?rank ?start)]
                    :order-by ?rank :limit 5 :offset 2]
            reader (d/prepare-q database query)]
        (doseq [start [2000 100]]
          (reset! pulled 0)
          (let [actual (reader [start pattern])
                pull-count @pulled]
            (is (= (conventional query database start [:name]) actual))
            (is (= (count actual) pull-count))))
        (doseq [start [4998 6000]]
          (is (= (conventional query database start [:name])
                 (reader [start [:name]])))))
      (let [query '[:find ?rank (pull ?e [:name])
                    :in $ ?start
                    :where [?e :rank ?rank] [(>= ?rank ?start)]
                    :order-by ?rank :limit 5]
            explain (d/explain {:run? true} query database 2000)]
        (is (true? (:access-path-selected? explain)))
        (is (= 5 (get-in explain [:plan :candidate-count])))
        (is (= 5 (count (:result explain))))))))

(deftest ordered-range-ties-and-filtered-pull-test
  (with-records
    (fn [conn _]
      ;; A page boundary lies inside a tied value group. The secondary order
      ;; must see all ties; identical pulled maps must not collapse entities.
      (d/transact! conn (mapv #(hash-map :db/id (+ 10000 %) :rank 2000
                                       :name "same" :keep true)
                             (range 12)))
      (let [database (db/-clear-tx-cache @conn)
            tied '[:find ?e ?rank (pull ?e [:name])
                   :in $ ?start
                   :where [?e :rank ?rank] [(>= ?rank ?start)]
                   :order-by [?rank :asc ?e :desc] :limit 5 :offset 2]
            projected '[:find ?rank (pull ?e [:name])
                        :in $ ?start
                        :where [?e :rank ?rank] [(>= ?rank ?start)]
                        :order-by ?rank :limit 5]
            filtered '[:find ?e ?rank (pull ?e [:name])
                       :in $ ?start
                       :where [?e :rank ?rank] [(>= ?rank ?start)]
                              [?e :keep true]
                       :order-by [?rank :asc ?e :asc] :limit 5 :offset 2]]
        (doseq [query [tied projected filtered]]
          (is (= (conventional query database 2000)
                 (d/q query database 2000))))
        (is (= (vec (repeat 5 [2000 {:name "same"}]))
               (d/q projected database 2000)))
        (is (true? (:access-path-selected?
                     (d/explain {} filtered database 2000))))
        (testing "a pull pattern computed by where clauses stays conventional"
          (let [query '[:find ?rank ?pattern (pull ?e ?pattern)
                        :where [?e :rank ?rank] [(>= ?rank 2000)]
                               [(ground [:name]) ?pattern]
                        :order-by ?rank :limit 5]]
            (is (false? (:access-path-selected? (d/explain {} query database))))
            (is (= (conventional query database) (d/q query database)))))
        (testing "global aggregation and ordering by a pull stay conventional"
          (doseq [query ['[:find (count ?e) ?rank
                          :where [?e :rank ?rank] [(>= ?rank 2000)]
                          :order-by ?rank :limit 5]
                         '[:find (pull ?e [:name]) ?rank
                           :where [?e :rank ?rank] [(>= ?rank 2000)]
                           :order-by [?rank :asc ?e :asc] :limit 5]]]
            (is (false? (:access-path-selected? (d/explain {} query database))))))))))
