(ns datalevin.storage.schema-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.storage.schema :as schema]
            [datalevin.validate :as vld]))

(deftest new-attribute-indexing-defaults
  (doseq [value-type [:db.type/idoc :db.type/vec]]
    (let [props {:db/valueType value-type}
          prepare #(get (schema/prepare-schema-update %1 {:value %2}) :value)]
      (is (= (assoc props :db/noindex true) (prepare {} props)))
      (doseq [noindex [true false]]
        (let [explicit (assoc props :db/noindex noindex)]
          (is (= explicit (prepare {} explicit)))))
      (doseq [unique [:db.unique/identity :db.unique/value]]
        (let [indexed (assoc props :db/unique unique)]
          (is (= indexed (prepare {} indexed)))
          (is (thrown? Exception
                       (vld/validate-schema
                         {:value (prepare {} (assoc indexed :db/noindex true))})))))
      (testing "existing attributes keep their effective indexing policy"
        (doseq [existing [props (assoc props :db/noindex true)
                          (assoc props :db/noindex false)]]
          (is (= existing (prepare {:value existing} props)))))
      (testing "adding a type to an existing attribute does not opt it out"
        (is (= props (prepare {:value {}} props))))
      (testing "explicit property retraction is not undone by defaulting"
        (is (= props (prepare {:value (assoc props :db/noindex true)}
                              {:db/noindex :db/retract}))))))
  (doseq [props [{} {:db/valueType :db.type/bytes}
                 {:db/valueType :db.type/string :db/fulltext true}
                 {:db/valueType :db.type/ref}]]
    (is (= {:value props} (schema/prepare-schema-update {} {:value props})))))
