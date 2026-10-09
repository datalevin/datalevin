(ns datalevin.test.subsystems
  "Public API smoke coverage for the native release runner."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(deftest shared-wal-is-rejected
  (let [dir (u/tmp-dir (str "shared-wal-rejected-" (random-uuid)))]
    (try
      (is (thrown-with-msg? Exception #"Shared-WAL mode is no longer supported"
            (d/open-kv dir {:wal? true :wal-shared? true})))
      (is (thrown-with-msg? Exception #"Shared-WAL mode is no longer supported"
            (d/get-conn dir {} {:wal? true :wal-shared? true})))
      (finally (u/delete-files dir)))))

(deftest kv-transactions-lists-prepared-operations-and-reopen
  (doseq [wal? [false true]]
    (testing (str "WAL=" wal?)
      (let [dir (u/tmp-dir (str "kv-smoke-" (random-uuid)))
            opts {:wal? wal?}]
        (try
          (let [db (d/open-kv dir opts)]
            (try
              (d/open-dbi db "counts")
              (d/open-list-dbi db "lists")
              (d/transact-kv db "counts" [[:put 1 10] [:put 2 20]] :long :long)
              (let [read! (d/prepare-get-value db "counts" :long :long)
                    update! (d/prepare-update-kv db "counts" + :long :long 5)]
                (is (= 10 (read! 1)))
                (update! 1)
                (is (= 15 (read! 1)))
                (d/with-transaction-kv [tx db]
                  (d/transact-kv tx "counts" [[:put 1 99]] :long :long)
                  (is (= 99 (d/get-value tx "counts" 1 :long :long)))
                  (d/abort-transact-kv tx))
                (is (= 15 (read! 1))))
              (is (= [[1 15] [2 20]]
                     (d/get-range db "counts" [:all] :long :long)))
              (d/put-list-items db "lists" 1 [3 1 2 2] :long :long)
              (is (= [1 2 3] (d/get-list db "lists" 1 :long :long)))
              (d/del-list-items db "lists" 1 [2] :long :long)
              (is (= [1 3] (d/get-list db "lists" 1 :long :long)))
              (when wal?
                (let [wm (d/txlog-watermarks db)]
                  (is (pos? (:last-committed-lsn wm)))
                  (is (= (:last-committed-lsn wm) (:last-durable-lsn wm)))))
              (finally (d/close-kv db))))
          (let [db (d/open-kv dir opts)]
            (try
              (is (= 15 (d/get-value db "counts" 1 :long :long)))
              (is (= [1 3] (d/get-list db "lists" 1 :long :long)))
              (finally (d/close-kv db))))
          (finally (u/delete-files dir)))))))

(deftest datalog-wal-upsert-prepared-query-and-reopen
  (let [dir (u/tmp-dir (str "datalog-smoke-" (random-uuid)))
        schema {:person/id {:db/valueType :db.type/long
                            :db/unique :db.unique/identity}
                :person/name {:db/valueType :db.type/string}}
        query '[:find ?name . :in $ ?id
                :where [?e :person/id ?id] [?e :person/name ?name]]]
    (try
      (let [conn (d/create-conn dir schema {:wal? true})]
        (try
          (d/transact! conn [{:person/id 1 :person/name "before"}])
          (let [eid (d/entid @conn [:person/id 1])
                read! (d/prepare-q @conn query)]
            (is (= "before" (read! [1])))
            (d/transact! conn [{:person/id 1 :person/name "after"}])
            (is (= eid (d/entid @conn [:person/id 1])))
            (is (= "after" (read! [1])))
            (is (= {:person/name "after"}
                   ((d/prepare-pull @conn [:person/name]) eid))))
          (finally (d/close conn))))
      (let [conn (d/create-conn dir schema {:wal? true})]
        (try (is (= "after" (d/q query @conn 1)))
             (finally (d/close conn))))
      (finally (u/delete-files dir)))))

(deftest fulltext-index-follows-datalog-updates
  (let [dir (u/tmp-dir (str "search-smoke-" (random-uuid)))
        conn (d/create-conn dir {:text {:db/valueType :db.type/string
                                       :db/fulltext true}})
        query '[:find ?e :in $ ?query
                :where [(fulltext $ ?query) [[?e _ _]]]]]
    (try
      (d/transact! conn [{:db/id 1 :text "hello world"}])
      (is (= #{[1]} (d/q query @conn "hello")))
      (d/transact! conn [[:db/add 1 :text "goodbye world"]])
      (is (= #{} (d/q query @conn "hello")))
      (is (= #{[1]} (d/q query @conn "goodbye")))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest idoc-map-and-vector-storage-and-matching
  (let [dir (u/tmp-dir (str "idoc-smoke-" (random-uuid)))
        conn (d/create-conn dir {:doc {:db/valueType :db.type/idoc}})
        query '[:find ?e :in $ ?query
                :where [(idoc-match $ :doc ?query) [[?e _ _]]]]]
    (try
      (d/transact! conn [{:db/id 1 :doc {:scores [1 5]}}
                         {:db/id 2 :doc [2 5]}])
      (is (= {:doc {:scores [1 5]}} (d/pull @conn [:doc] 1)))
      (is (= {:doc [2 5]} (d/pull @conn [:doc] 2)))
      (is (= #{[1]} (d/q query @conn {:scores 5})))
      (is (= #{[2]} (d/q query @conn 5)))
      (d/transact! conn [[:db/add 2 :doc [2 3]]])
      (is (= #{} (d/q query @conn 5)))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest vector-add-search-remove-and-reopen
  (let [dir (u/tmp-dir (str "vector-smoke-" (random-uuid)))
        db (d/open-kv dir)]
    (try
      (let [index (d/new-vector-index db {:dimensions 2})]
        (try
          (d/add-vec index :a [1.0 0.0])
          (d/add-vec index :b [0.0 1.0])
          (is (= [:a] (d/search-vec index [1.0 0.0] {:top 1})))
          (d/remove-vec index :a)
          (is (= [:b] (d/search-vec index [1.0 0.0] {:top 1})))
          (finally (d/close-vector-index index))))
      (let [index (d/new-vector-index db {:dimensions 2})]
        (try (is (= [:b] (d/search-vec index [0.0 1.0] {:top 1})))
             (finally (d/close-vector-index index))))
      (finally (d/close-kv db) (u/delete-files dir)))))
