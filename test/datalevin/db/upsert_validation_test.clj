(ns datalevin.db.upsert-validation-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.db.tx.prepare :as prepare]
            [datalevin.test.core :refer [db-fixture]]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:name {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :aliases {:db/valueType :db.type/string :db/unique :db.unique/identity
             :db/cardinality :db.cardinality/many}
   :flag {:db/valueType :db.type/boolean :db/unique :db.unique/identity}
   :value {:db/valueType :db.type/long}})

(deftest upsert-validation-preserves-scalars-members-and-syntax-order
  (with-open [conn (d/create-conn nil schema
                    {:wal? false :snapshot-scheduler? false :background-sampling? false})]
    (d/transact! conn [{:db/id 1 :name "one" :aliases ["known"] :flag false}])
    (is (= [{:db/id -1 :aliases ["new"] :value 2}
            {:aliases {"known" 1}}]
           (prepare/resolve-upserts @conn
             {:db/id -1 :aliases ["known" "new"] :value 2})))
    (is (= [{:db/id -1} {:flag {false 1}}]
           (prepare/resolve-upserts @conn {:db/id -1 :flag false})))
    (is (= [{:db/id -1} {}]
           (prepare/resolve-upserts @conn {:db/id -1 :aliases []})))
    (doseq [entity [{:db/id -1 :name nil}
                    {:db/id -1 :aliases nil}
                    {:db/id -1 :value nil}
                    {:db/id nil}
                    {:db/id -1 :aliases [nil]}
                    {:db/id -1 :aliases ["known" nil]}
                    {:db/id -1 :aliases '("known" nil)}
                    {:db/id -1 :aliases #{"known" nil}}
                    {:db/id -1 :aliases (object-array ["known" nil])}]]
      (let [error (try (prepare/resolve-upserts @conn entity)
                      nil
                      (catch clojure.lang.ExceptionInfo e e))]
        (is (= {:error :transact/syntax :value nil :context entity}
               (ex-data error)))))
    (let [entity (array-map "bad-attribute" nil)
          error (try (prepare/resolve-upserts @conn entity)
                     nil
                     (catch clojure.lang.ExceptionInfo e e))]
      (is (= {:error :transact/syntax :attribute "bad-attribute" :context entity}
             (ex-data error))))))
