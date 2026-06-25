(ns datalevin.test.datafy
  (:require
   [clojure.datafy :as datafy]
   [clojure.test :refer [deftest is testing]]
   [datalevin.core :as d]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(defn- nav
  [coll k]
  (datafy/nav coll k (coll k)))

(defn- d+n
  [x ks]
  (reduce (fn [coll k] (datafy/datafy (nav coll k)))
          (datafy/datafy x)
          ks))

(deftest test-navigation
  (let [dir (u/tmp-dir (str "datafy-navigation-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir
                  {:name          {}
                   :ref           {:db/valueType :db.type/ref}
                   :namespace/ref {:db/valueType :db.type/ref}
                   :many/ref      {:db/valueType   :db.type/ref
                                   :db/cardinality :db.cardinality/many}})
                (d/db-with [{:db/id         1
                             :name          "root"
                             :namespace/ref 4}
                            {:db/id    2
                             :name     "many-source"
                             :ref      5
                             :many/ref [1 2 3]}
                            {:db/id         3
                             :name          "reverse-namespace"
                             :namespace/ref 2}
                            {:db/id 4
                             :name  "forward"
                             :ref   5}
                            {:db/id 5
                             :name  "reverse-target"}]))
        entity (d/entity db 1)]
    (try
      (is (= 3 (:db/id (d+n entity [:namespace/ref :ref :_ref
                                    0 :namespace/_ref 0]))))
      (is (= #{1 2 3}
             (set (map :db/id (d+n entity [:many/_ref 0 :many/ref])))))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest datafy-entity-cardinality-many-values-are-sets
  (let [dir (u/tmp-dir (str "datafy-entity-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir
                  {:name    {:db/unique :db.unique/identity}
                   :aka     {:db/cardinality :db.cardinality/many}
                   :friends {:db/valueType   :db.type/ref
                             :db/cardinality :db.cardinality/many}
                   :part    {:db/valueType   :db.type/ref
                             :db/cardinality :db.cardinality/many
                             :db/isComponent true}})
                (d/db-with [{:db/id   1
                             :name    "Ivan"
                             :aka     ["I" "V"]
                             :friends [{:name "Petr"} {:name "Oleg"}]
                             :part    [{:name "Child"
                                        :aka  ["C1" "C2"]}]}]))]
    (try
      (let [ivan (datafy/datafy (d/touch (d/entity db 1)))
            petr (datafy/datafy (d/entity db [:name "Petr"]))]
        (is (map? ivan))

        (testing "scalar cardinality-many"
          (is (= #{"I" "V"} (:aka ivan))))

        (testing "ref cardinality-many"
          (is (= #{{:db/id 2} {:db/id 3}} (:friends ivan))))

        (testing "nested component cardinality-many"
          (is (= #{{:db/id 4
                    :name  "Child"
                    :aka   #{"C1" "C2"}}}
                 (:part ivan))))

        (testing "reverse ref cardinality-many"
          (is (= #{{:db/id 1}} (:_friends petr)))))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))
