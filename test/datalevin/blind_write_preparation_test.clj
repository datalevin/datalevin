(ns datalevin.blind-write-preparation-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:key {:db/valueType :db.type/long :db/unique :db.unique/identity}
   :value {:db/valueType :db.type/string}})

(deftest cached-attribute-eligibility-retains-per-value-bounds
  (let [dir (u/tmp-dir (str "blind-attr-bounds-" (random-uuid)))
        conn (d/create-conn dir schema)]
    (try
      (let [short {:key 1 :value "short"}
            long-value {:key 2 :value (apply str (repeat 101 "x"))}]
        (testing "a later value can disqualify an attribute already cached"
          (doseq [entities [[short long-value] [long-value short]]]
            (let [prepared (db/prepare-blind-local-tx @conn entities true false)]
              (is (some? prepared))
              (is (false? (:fuse-unique-inserts? prepared)))
              (is (= 2 (count (:unique-avs prepared)))))))
        (testing "eligibility from a preceding request does not leak"
          (is (true? (:fuse-unique-inserts?
                       (db/prepare-blind-local-tx
                         @conn [short {:key 2 :value "shorter"}] true false))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest cached-attribute-eligibility-follows-schema-and-validation
  (let [dir (u/tmp-dir (str "blind-attr-schema-" (random-uuid)))
        conn (d/create-conn dir schema)
        entities [{:key 1 :value "one"} {:key 2 :value "two"}]
        prepare #(db/prepare-blind-local-tx @conn % true false)]
    (try
      (is (true? (:fuse-unique-inserts? (prepare entities))))
      (d/update-schema conn {:value {:db/noindex true}})
      (is (false? (:fuse-unique-inserts? (prepare entities))))
      (d/update-schema conn {:value {:db/noindex false}})
      (is (true? (:fuse-unique-inserts? (prepare entities))))
      (testing "duplicate identities still require general resolution"
        (is (nil? (db/prepare-blind-local-tx
                    @conn [{:key 1} {:key 1}] true true))))
      (testing "an unsupported attribute after a supported entity still rejects"
        (is (nil? (prepare [(first entities) {:key 2 :unknown "value"}]))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
