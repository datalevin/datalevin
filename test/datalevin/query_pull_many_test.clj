(ns datalevin.query-pull-many-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.pull-api :as pull]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.timeout :as timeout]))

(use-fixtures :each db-fixture)

(deftest ordered-range-pulls-preserve-the-selected-window
  (let [conn (d/create-conn nil
                           {:key {:db/valueType :db.type/string
                                  :db/unique :db.unique/value}
                            :value {:db/valueType :db.type/string
                                    :db/noindex true}}
                           {:cache-limit 0 :kv-opts {:inmemory? true}})
        key-for #(format "user%04d" %)
        query '{:find [?key (pull ?e pattern) (pull ?e [:value])]
                :in [$ ?start pattern]
                :where [[?e :key ?key] [(>= ?key ?start)]]
                :order-by [?key] :offset 2 :limit 3}
        expected (fn [start]
                   (mapv (fn [n]
                           [(key-for n) {:db/id (- 1000 n) :value "same"}
                            {:value "same"}])
                         (take 3 (drop 2 (range start 1000)))))]
    (try
      ;; Key order and entity ID order deliberately disagree. Equal payloads
      ;; must retain their separate positions in the selected result window.
      (d/transact! conn
                   (mapv (fn [n]
                           {:db/id (- 1000 n) :key (key-for n) :value "same"})
                         (range 1000)))
      (let [plan (d/explain {:run? true} query @conn (key-for 100) [:db/id :value])
            reader (d/prepare-q @conn query)]
        (is (:access-path-selected? plan))
        (is (= :ave (get-in plan [:plan :access-plan :method])))
        (is (= (expected 100) (:result plan)))
        (doseq [start [100 995 997 999 1000]]
          (is (= (expected start)
                 (d/q query @conn (key-for start) [:db/id :value])))
          (is (= (expected start)
                 (reader [(key-for start) [:db/id :value]])))))
      (testing "missing projections keep their place beside other columns"
        (is (= (mapv (fn [[key _ value]] [key nil value]) (expected 100))
               (d/q query @conn (key-for 100) [:absent]))))
      (finally (d/close conn)))))

(deftest pull-many-inherits-query-deadlines
  (let [conn (d/create-conn nil {:name {}} {:kv-opts {:inmemory? true}})]
    (try
      (d/transact! conn [{:db/id 1 :name "one"}])
      (binding [timeout/*deadline* 1]
        (doseq [opts [{} {:timeout 10000}]]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"took too long"
                               (pull/pull-many @conn [:name] [1] opts))))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"took too long"
                             (d/entity-range @conn 0 2)))
        (is (= 1 timeout/*deadline*)))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"took too long"
                           (pull/pull-many @conn [:name] [1] {:timeout -1})))
      (is (= [{:name "one"}] (pull/pull-many @conn [:name] [1])))
      (finally (d/close conn)))))
