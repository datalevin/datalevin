(ns datalevin.noindex-migration-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interpret :as inter]))

(deftest unindexed-type-migration-does-not-read-other-custom-payloads
  (let [conn (d/create-conn nil {:data {:db/noindex true}} {:wal? true})]
    (try
      (d/register-type
        conn :app/value
        {:index {:type :long :order-fn (inter/inter-fn [v] (:rank v))}
         :payload {:serialize (inter/inter-fn [v] (byte-array [(:rank v)]))
                   :deserialize (inter/inter-fn [_]
                                  (throw (ex-info "Unrelated custom payload read" {})))}})
      (d/update-schema conn {:custom {:db/valueType :app/value :db/noindex true}})
      (d/transact! conn [{:db/id 1 :data 42}
                         {:db/id 2 :custom {:rank 1}}])
      (d/update-schema conn {:data {:db/valueType :db.type/long}})
      (is (= 42 (:data (d/entity @conn 1))))
      (is (= :db.type/long (get-in (d/schema conn) [:data :db/valueType])))
      (is (= 1 (d/entries (d/datalog-kv conn) c/custom-values)))
      (finally (d/close conn)))))
