(ns datalevin.db.blind-many-nil-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.test.core :refer [db-fixture]]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:key {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :value {:db/valueType :db.type/string}
   :tags {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}
   :flags {:db/valueType :db.type/boolean :db/cardinality :db.cardinality/many}})

(def ^:private options
  {:background-sampling? false :snapshot-scheduler? false})

(def ^:private invalid-values
  [[nil] [nil "a"] ["a" nil "b"] ["a" nil] ["a" "a" nil "b"]])

(deftest every-many-element-is-validated-during-blind-preparation
  (doseq [validate? [false true]]
    (with-open [conn (d/create-conn nil schema (assoc options :validate-data? validate?))]
      (doseq [prefix-size [0 32] collect? [false true]
              values invalid-values wrap [vec #(apply list %) #(map identity %)]]
        (testing (str [validate? prefix-size collect? values])
          (let [forms (conj (vec (repeat prefix-size {:value "prefix"}))
                            {:tags (wrap values)})]
            (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Cannot store nil as a value"
                                  (db/prepare-blind-local-tx @conn forms true collect?)))))))))

(deftest nil-in-many-inserts-rejects-the-whole-transaction
  (doseq [wal? [false true] explicit? [false true]]
    (testing (str [wal? explicit?])
      (with-open [conn (d/create-conn nil schema (assoc options :wal? wal?))]
        (d/transact! conn [{:db/id 1 :value "before" :tags ["existing"]}])
        (let [before (vec (d/datoms @conn :eav))
              max-before (select-keys @conn [:max-eid :max-tx])]
          (doseq [prefix-size [0 32] values invalid-values]
            (let [forms (conj (vec (repeat prefix-size {:value "prefix"}))
                              {:db/id "bad" :key "invalid" :tags values})]
              (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Cannot store nil as a value"
                                    (if explicit?
                                      (d/with-transaction [tx conn]
                                        (d/transact! tx [[:db/add 1 :value "staged"]])
                                        (d/transact! tx forms))
                                      (d/transact! conn forms))))
              (is (= before (vec (d/datoms @conn :eav))))
              (is (= max-before (select-keys @conn [:max-eid :max-tx])))))
          (testing "the writer remains usable and false is a value"
            (d/transact! conn [{:key "valid" :tags [] :flags [false true false]}])
            (is (= #{false true} (:flags (d/entity @conn [:key "valid"]))))
            (is (nil? (:tags (d/entity @conn [:key "valid"]))))))))))
