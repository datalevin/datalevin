(ns datalevin.test.datafy
  (:require
   [clojure.datafy :as datafy]
   [clojure.test :refer [deftest is testing]]
   [datalevin.core :as d]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

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
