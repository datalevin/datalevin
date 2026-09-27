(ns datalevin.schema-transfer-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.core :as d]
   [datalevin.interface :as i]
   [datalevin.storage :as s]
   [datalevin.util :as u])
  (:import [datalevin.storage Store]))

(def ^:private document-schema
  {:doc/profile {:db/valueType :db.type/idoc :db/domain "profiles"}})

(defn- matching-documents [conn]
  (set (d/q '[:find [?e ...]
              :in $ ?query
              :where
              [(idoc-match $ :doc/profile ?query) [[?e ?a ?v]]]]
            @conn {:status "active"})))

(deftest schema-transfer-initializes-committed-document-domains
  (doseq [wal? [false true]]
    (let [dir (u/tmp-dir (str "schema-transfer-" (random-uuid)))
          conn (d/create-conn dir nil {:wal? wal?})
          committed-schema (volatile! nil)]
      (try
        (d/with-transaction [tx conn]
          (d/with-transaction [nested tx]
            (d/update-schema nested document-schema))
          (vreset! committed-schema (i/schema (:store @tx)))
          ;; New document DBIs must wait for the outer native commit.
          (is (nil? (get (s/store-idoc-indices (:store @tx)) "profiles"))))
        (is (identical? @committed-schema (i/schema (:store @conn))))
        (is (some? (get (s/store-idoc-indices (:store @conn)) "profiles")))
        (let [store (:store @conn)]
          (d/transact! conn [{:db/id 1 :doc/profile {:status "active"}}])
          (testing "ordinary writes still reuse immutable schema state"
            (doseq [derived [i/schema i/rschema i/attrs]]
              (is (identical? (derived store) (derived (:store @conn)))))))
        (is (= #{1} (matching-documents conn)))
        (d/close conn)
        (let [reopened (d/create-conn dir nil {:wal? wal?})]
          (try
            (is (= #{1} (matching-documents reopened)))
            (finally (d/close reopened))))
        (finally
          (when-not (d/closed? conn) (d/close conn))
          (u/delete-files dir))))))

(deftest schema-transfer-does-not-initialize-aborted-document-domains
  (doseq [wal? [false true]
          abort [d/abort-transact
                 (fn [_] (throw (ex-info "abort schema update" {})))]]
    (let [dir (u/tmp-dir (str "schema-transfer-abort-" (random-uuid)))
          conn (d/create-conn dir {:item/value {:db/valueType :db.type/long}}
                              {:wal? wal?})
          before (d/schema conn)
          lmdb (.-lmdb ^Store (:store @conn))
          dbis (set (i/list-dbis lmdb))]
      (try
        (try
          (d/with-transaction [tx conn]
            (d/update-schema tx document-schema)
            (abort tx))
          (catch clojure.lang.ExceptionInfo e
            (is (= "abort schema update" (ex-message e)))))
        (is (= before (d/schema conn)))
        (is (nil? (get (s/store-idoc-indices (:store @conn)) "profiles")))
        (is (= dbis (set (i/list-dbis lmdb))))
        (d/transact! conn [{:db/id 1 :item/value 42}])
        (is (= 42 (:item/value (d/entity @conn 1))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))
