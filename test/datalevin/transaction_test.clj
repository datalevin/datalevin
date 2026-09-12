(ns datalevin.transaction-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.test.core :as test-core]
            [datalevin.util :as u]))

(use-fixtures :each test-core/db-fixture)

(deftest in-memory-transaction-queries
  (let [schema {:fact/id {:db/valueType :db.type/uuid
                          :db/unique :db.unique/identity}
                :fact/kind {:db/valueType :db.type/keyword}
                :fact/subject {:db/valueType :db.type/string}
                :fact/status {:db/valueType :db.type/keyword}}
        conn (d/create-conn nil schema {:kv-opts {:inmemory? true :wal? false}})
        fact-id (random-uuid)
        query '[:find [?id ...]
                :in $ ?topic
                :where [?f :fact/kind :fact.kind/docs-gap]
                       [?f :fact/subject ?topic]
                       [?f :fact/status :fact.status/current]
                       [?f :fact/id ?id]]
        current-ids #(d/q query (d/db %) "backup and restore")]
    (try
      (is (empty? (current-ids conn)))
      (d/with-transaction [tx conn]
        (is (empty? (current-ids tx)))
        (d/transact! tx [{:fact/id fact-id
                          :fact/kind :fact.kind/docs-gap
                          :fact/subject "backup and restore"
                          :fact/status :fact.status/current}])
        (is (= [fact-id] (current-ids tx))))
      (is (= [fact-id] (current-ids conn)))
      (d/with-transaction [tx conn]
        (is (= [fact-id] (current-ids tx)))
        (d/transact! tx [{:fact/id fact-id
                          :fact/status :fact.status/superseded}])
        (is (empty? (current-ids tx))))
      (is (empty? (current-ids conn)))
      (finally
        (d/close conn)))))

(deftest list-ranges-in-write-transactions
  (doseq [opts [{:inmemory? true} {:temp? true} {}]]
    (testing (str "environment options " opts)
      (let [dir (u/tmp-dir (str "transaction-list-range-" (random-uuid)))
            kv (d/open-kv dir (assoc opts :wal? false))
            first-item #(d/list-range-first % "items" [:closed 1 1] :long
                                             [:closed 2 4] :long)]
        (try
          (d/open-list-dbi kv "items")
          (d/with-transaction-kv [tx kv]
            (is (nil? (first-item tx)))
            (d/put-list-items tx "items" 1 [1 2 3 4 5] :long :long)
            (is (= [1 2] (first-item tx)))
            (is (= [[1 2] [1 3] [1 4]]
                   (d/list-range tx "items" [:closed 1 1] :long
                                            [:closed 2 4] :long))))
          (d/with-transaction-kv [tx kv]
            (is (= [1 2] (first-item tx))))
          (is (= [1 2] (first-item kv)))
          (finally
            (d/close-kv kv)
            (when (.exists (u/file dir))
              (u/delete-files dir))))))))
