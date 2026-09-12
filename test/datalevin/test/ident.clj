(ns datalevin.test.ident
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.util :as u]
   [datalevin.core :as d])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest test-q
  (let [dir (u/tmp-dir (str "q-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:ref {:db/valueType :db.type/ref}})
                (d/db-with [[:db/add 1 :db/ident :ent1]
                            [:db/add 2 :db/ident :ent2]
                            [:db/add 2 :ref 1]]))]
    (is (= 1 (d/q '[:find ?v .
                    :where [:ent2 :ref ?v]] db)))
    (is (= 2 (d/q '[:find ?f .
                    :where [?f :ref :ent1]] db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-transact!
  (let [dir (u/tmp-dir (str "tx-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:ref {:db/valueType :db.type/ref}})
                (d/db-with [[:db/add 1 :db/ident :ent1]
                            [:db/add 2 :db/ident :ent2]
                            [:db/add 2 :ref 1]]))
        db1 (d/db-with db [[:db/add :ent1 :ref :ent2]])]
    (is (= 2 (-> (d/entity db1 :ent1) :ref :db/id)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-entity
  (let [dir (u/tmp-dir (str "ent-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:ref {:db/valueType :db.type/ref}})
                (d/db-with [[:db/add 1 :db/ident :ent1]
                            [:db/add 2 :db/ident :ent2]
                            [:db/add 2 :ref 1]]))]
    (is (= {:db/ident :ent1}
           (into {} (d/entity db :ent1))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-pull
  (let [dir (u/tmp-dir (str "pull-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:ref {:db/valueType :db.type/ref}})
                (d/db-with [[:db/add 1 :db/ident :ent1]
                            [:db/add 2 :db/ident :ent2]
                            [:db/add 2 :ref 1]]))]
    (is (= {:db/id 1, :db/ident :ent1}
           (d/pull db '[*] :ent1)))
    (d/close-db db)
    (u/delete-files dir)))
