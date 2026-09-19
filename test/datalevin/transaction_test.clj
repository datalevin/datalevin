(ns datalevin.transaction-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.constants :as c]
            [datalevin.lmdb :as l]
            [datalevin.test.core :as test-core]
            [datalevin.util :as u]))

(use-fixtures :each test-core/db-fixture)

(deftest nested-transact-reuses-store-and-publishes-local-results
  (doseq [wal? [false true]
          prepare? [false true]]
    (testing (str "WAL " wal? ", prepare " prepare?)
      (binding [c/*use-prepare-path* prepare?]
        (let [dir (u/tmp-dir (str "nested-transact-" (random-uuid)))
              schema {:name {:db/unique :db.unique/identity}
                      :value {:db/valueType :db.type/long}}
              conn (d/create-conn dir schema {:kv-opts {:wal? wal?}})]
          (try
            (d/transact! conn [{:db/id 1 :name "one" :value 0}])
            (d/with-transaction [tx conn]
              (let [before @tx
                    report (d/transact! tx [[:db/add 1 :value 1]] {:step 1})]
                (is (identical? (:store before) (:store @tx)))
                (is (identical? before (:db-before report)))
                (is (identical? @tx (:db-after report)))
                (is (not (identical? (:eavt before) (:eavt @tx))))
                (is (= {:step 1} (:tx-meta report)))
                (is (= 1 (:value (d/pull @tx [:value] 1))))
                (let [next-report (d/transact! tx [{:name "two" :value 2}])
                      eid (d/q '[:find ?e . :where [?e :name "two"]] @tx)]
                  (is (< 1 eid))
                  (is (< (get-in report [:tempids :db/current-tx])
                         (get-in next-report [:tempids :db/current-tx])))
                  (is (= 2 (:value (d/pull @tx [:value] eid))))
                  (d/transact! tx [[:db/retractEntity eid]])
                  (is (nil? (d/pull @tx [:value] eid)))))
              (d/with-transaction [inner tx]
                (d/transact! inner [[:db/add 1 :value 3]]))
              (is (= 3 (:value (d/pull @tx [:value] 1)))))
            (is (= 3 (:value (d/pull @conn [:value] 1))))
            (d/close conn)
            (let [reopened (d/create-conn dir)]
              (try
                (is (= 3 (:value (d/pull @reopened [:value] 1))))
                (is (= 1 (d/q '[:find (count ?e) . :where [?e :name]] @reopened)))
                (finally (d/close reopened))))
            (finally (d/close conn) (u/delete-files dir))))))))

(deftest nested-transact-abort-and-caught-validation-error
  (doseq [wal? [false true]]
    (let [dir (u/tmp-dir (str "nested-abort-" (random-uuid)))
          conn (d/create-conn dir {:name {:db/unique :db.unique/identity}
                                  :value {:db/valueType :db.type/long}}
                              {:kv-opts {:wal? wal?}})]
      (try
        (d/transact! conn [{:db/id 1 :name "one" :value 0}])
        (d/with-transaction [tx conn]
          (let [before @tx]
            (is (thrown? Exception
                         (d/transact! tx [[:db/add 1 :value 1]
                                          [:db/add 1 :value "wrong type"]])))
            (is (identical? before @tx))
            (is (= 0 (:value (d/pull @tx [:value] 1)))))
          (d/transact! tx [[:db/add 1 :value 2]])
          (is (= 2 (:value (d/pull @tx [:value] 1))))
          (d/abort-transact tx))
        (is (= 0 (:value (d/pull @conn [:value] 1))))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"rollback"
              (d/with-transaction [tx conn]
                (d/transact! tx [[:db/add 1 :value 4]])
                (throw (ex-info "rollback" {})))))
        (is (= 0 (:value (d/pull @conn [:value] 1))))
        (d/with-transaction [tx conn]
          (d/update-schema tx {:extra {:db/valueType :db.type/string}})
          (d/transact! tx [[:db/add 1 :extra "new schema"]]))
        (is (= "new schema" (:extra (d/pull @conn [:extra] 1))))
        (finally (d/close conn) (u/delete-files dir))))))

(deftest nested-transact-preserves-global-timeout-wrapper
  (let [conn (d/create-conn nil {:value {}} {:kv-opts {:inmemory? true :wal? false}})
        previous (l/explicit-transaction-timeout)]
    (try
      (l/set-explicit-transaction-timeout! 5000)
      (d/with-transaction [tx conn]
        (d/transact! tx [{:db/id 1 :value 1}])
        (is (= 1 (:value (d/pull @tx [:value] 1)))))
      (is (= 1 (:value (d/pull @conn [:value] 1))))
      (finally
        (l/set-explicit-transaction-timeout! previous)
        (d/close conn)))))

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
