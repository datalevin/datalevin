(ns datalevin.test.validate
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.validate :as vld]
   [datalevin.util :as u])
  (:import
   [java.util UUID ArrayList]
   [datalevin.storage Store]
   [datalevin.lmdb KVTxData]))

(use-fixtures :each db-fixture)

;; ---- Storage / schema validators ----

(deftest test-validate-closed-schema
  (testing "passes when attr is defined or schema is not closed"
    (let [schema {:name {:db/valueType :db.type/string}}
          opts   {:closed-schema? false}]
      (is (nil? (vld/validate-closed-schema schema opts :name "test")))
      (is (nil? (vld/validate-closed-schema schema opts :unknown "test")))))

  (testing "passes when attr is defined and schema is closed"
    (let [schema {:name {:db/valueType :db.type/string}}
          opts   {:closed-schema? true}]
      (is (nil? (vld/validate-closed-schema schema opts :name "test")))))

  (testing "raises when attr is missing and schema is closed"
    (let [schema {:name {:db/valueType :db.type/string}}
          opts   {:closed-schema? true}]
      (is (thrown-with-msg? Exception #"closed-schema"
            (vld/validate-closed-schema schema opts :unknown "test"))))))

(deftest test-validate-cardinality-change
  (testing "passes for one->many or same cardinality"
    (let [dir  (u/tmp-dir (str "card-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (nil? (vld/validate-cardinality-change
                      store :name :db.cardinality/one :db.cardinality/many)))
          (is (nil? (vld/validate-cardinality-change
                      store :name :db.cardinality/one :db.cardinality/one))))
        (finally
          (d/close conn)
          (u/delete-files dir)))))

  (testing "raises for many->one when data exist"
    (let [dir  (u/tmp-dir (str "card-raise-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType   :db.type/string
                                          :db/cardinality :db.cardinality/many}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (d/transact! conn [{:name "Alice"}])
        (let [store (:store (d/db conn))]
          (is (thrown-with-msg? Exception #"Cardinality change is not allowed"
                (vld/validate-cardinality-change
                  store :name :db.cardinality/many :db.cardinality/one))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-value-type-change
  (testing "passes for same type"
    (let [dir  (u/tmp-dir (str "vt-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (nil? (vld/validate-value-type-change
                      store :name :db.type/string :db.type/string))))
        (finally
          (d/close conn)
          (u/delete-files dir)))))

  (testing "raises for different type when data exist"
    (let [dir  (u/tmp-dir (str "vt-raise-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (d/transact! conn [{:name "Alice"}])
        (let [store (:store (d/db conn))]
          (is (thrown-with-msg? Exception #"Value type change is not allowed"
                (vld/validate-value-type-change
                  store :name :db.type/string :db.type/long))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-uniqueness-change
  (testing "passes when no duplicates or no data"
    (let [dir  (u/tmp-dir (str "uniq-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))
              lmdb  (.-lmdb ^Store store)]
          ;; no data, should pass
          (is (nil? (vld/validate-uniqueness-change
                      store lmdb :name nil :db.unique/identity))))
        (finally
          (d/close conn)
          (u/delete-files dir)))))

  (testing "passes when adding unique and all values are distinct"
    (let [dir  (u/tmp-dir (str "uniq-ok-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (d/transact! conn [{:name "Alice"} {:name "Bob"}])
        (let [store (:store (d/db conn))
              lmdb  (.-lmdb ^Store store)]
          (is (nil? (vld/validate-uniqueness-change
                      store lmdb :name nil :db.unique/identity))))
        (finally
          (d/close conn)
          (u/delete-files dir)))))

  (testing "raises when adding unique and duplicates exist"
    (let [dir  (u/tmp-dir (str "uniq-raise-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (d/transact! conn [{:name "Alice"} {:name "Alice"}])
        (let [store (:store (d/db conn))
              lmdb  (.-lmdb ^Store store)]
          (is (thrown-with-msg? Exception #"uniqueness change is inconsistent"
                (vld/validate-uniqueness-change
                  store lmdb :name nil :db.unique/identity))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-schema-mutation
  (testing "dispatcher delegates correctly"
    (let [dir  (u/tmp-dir (str "schema-mut-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))
              lmdb  (.-lmdb ^Store store)]
          ;; should pass: value type same, cardinality same
          (is (nil? (vld/validate-schema-mutation
                      store lmdb :name
                      {:db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                      {:db/valueType :db.type/string :db/cardinality :db.cardinality/one}))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-key-size
  (testing "passes for small keys"
    (is (nil? (vld/validate-key-size "hello" :data)))
    (is (nil? (vld/validate-key-size 42 :long)))
    (is (nil? (vld/validate-key-size 42 :id))))

  (testing "raises for keys > 511 bytes"
    (let [big-key (apply str (repeat 600 "x"))]
      (is (thrown-with-msg? Exception #"511 bytes"
            (vld/validate-key-size big-key :data))))))

;; ---- KV validators ----

(deftest test-validate-kv-op
  (testing "valid ops pass without error"
    (is (nil? (vld/validate-kv-op :put)))
    (is (nil? (vld/validate-kv-op :del)))
    (is (nil? (vld/validate-kv-op :put-list)))
    (is (nil? (vld/validate-kv-op :del-list))))

  (testing "unknown op raises"
    (is (thrown-with-msg? Exception #"Unknown kv transact operator"
          (vld/validate-kv-op :bogus)))
    (is (thrown-with-msg? Exception #"Unknown kv transact operator"
          (vld/validate-kv-op :update)))))

(deftest test-validate-kv-key
  (testing "valid key passes"
    (is (nil? (vld/validate-kv-key "hello" :data false)))
    (is (nil? (vld/validate-kv-key 42 :long false)))
    (is (nil? (vld/validate-kv-key "hello" :data true))))

  (testing "nil key raises"
    (is (thrown-with-msg? Exception #"Key cannot be nil"
          (vld/validate-kv-key nil :data false)))
    (is (thrown-with-msg? Exception #"Key cannot be nil"
          (vld/validate-kv-key nil :data true))))

  (testing "type mismatch raises when validate-data? is true"
    (is (thrown-with-msg? Exception #"Invalid data, expecting"
          (vld/validate-kv-key "hello" :long true))))

  (testing "type mismatch ignored when validate-data? is false"
    (is (nil? (vld/validate-kv-key "hello" :long false)))))

(deftest test-validate-kv-value
  (testing "valid value passes"
    (is (nil? (vld/validate-kv-value "hello" :data false)))
    (is (nil? (vld/validate-kv-value 42 :long false)))
    (is (nil? (vld/validate-kv-value "hello" :data true))))

  (testing "nil value raises"
    (is (thrown-with-msg? Exception #"Value cannot be nil"
          (vld/validate-kv-value nil :data false)))
    (is (thrown-with-msg? Exception #"Value cannot be nil"
          (vld/validate-kv-value nil :data true))))

  (testing "type mismatch raises when validate-data? is true"
    (is (thrown-with-msg? Exception #"Invalid data, expecting"
          (vld/validate-kv-value "hello" :long true))))

  (testing "type mismatch ignored when validate-data? is false"
    (is (nil? (vld/validate-kv-value "hello" :long false)))))

(deftest test-validate-kv-tx-data
  (testing "valid :put tx passes"
    (let [tx (KVTxData. :put "test-dbi" "mykey" "myval" :data :data nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "valid :del tx passes"
    (let [tx (KVTxData. :del "test-dbi" "mykey" nil :data nil nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "valid :put-list tx passes"
    (let [tx (KVTxData. :put-list "test-dbi" "mykey" ["v1" "v2"] :data :data nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "valid :del-list tx passes"
    (let [tx (KVTxData. :del-list "test-dbi" "mykey" ["v1" "v2"] :data :data nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "java.util.List values pass for :put-list"
    (let [jlist (doto (ArrayList.) (.add "v1") (.add "v2"))
          tx    (KVTxData. :put-list "test-dbi" "mykey" jlist :data :data nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "java.util.List values pass for :del-list"
    (let [jlist (doto (ArrayList.) (.add "v1") (.add "v2"))
          tx    (KVTxData. :del-list "test-dbi" "mykey" jlist :data :data nil)]
      (is (nil? (vld/validate-kv-tx-data tx false)))))

  (testing "unknown op raises"
    (let [tx (KVTxData. :bogus "test-dbi" "mykey" "myval" :data :data nil)]
      (is (thrown-with-msg? Exception #"Unknown kv transact operator"
            (vld/validate-kv-tx-data tx false)))))

  (testing "nil key raises"
    (let [tx (KVTxData. :put "test-dbi" nil "myval" :data :data nil)]
      (is (thrown-with-msg? Exception #"Key cannot be nil"
            (vld/validate-kv-tx-data tx false)))))

  (testing "nil value in :put raises"
    (let [tx (KVTxData. :put "test-dbi" "mykey" nil :data :data nil)]
      (is (thrown-with-msg? Exception #"Value cannot be nil"
            (vld/validate-kv-tx-data tx false)))))

  (testing "oversized key raises"
    (let [big-key (apply str (repeat 600 "x"))
          tx      (KVTxData. :put "test-dbi" big-key "myval" :data :data nil)]
      (is (thrown-with-msg? Exception #"511 bytes"
            (vld/validate-kv-tx-data tx false)))))

  (testing "type mismatch raises when validate-data? is true"
    (let [tx (KVTxData. :put "test-dbi" "mykey" "myval" :long :data nil)]
      (is (thrown-with-msg? Exception #"Invalid data, expecting"
            (vld/validate-kv-tx-data tx true)))))

  (testing "nil value in :put-list raises"
    (let [tx (KVTxData. :put-list "test-dbi" "mykey" nil :data :data nil)]
      (is (thrown-with-msg? Exception #"sequential collection"
            (vld/validate-kv-tx-data tx false)))))

  (testing "non-sequential value in :put-list raises"
    (let [tx (KVTxData. :put-list "test-dbi" "mykey" "scalar" :data :data nil)]
      (is (thrown-with-msg? Exception #"sequential collection"
            (vld/validate-kv-tx-data tx false)))))

  (testing "nil value in :del-list raises"
    (let [tx (KVTxData. :del-list "test-dbi" "mykey" nil :data :data nil)]
      (is (thrown-with-msg? Exception #"sequential collection"
            (vld/validate-kv-tx-data tx false)))))

  (testing "non-sequential value in :del-list raises"
    (let [tx (KVTxData. :del-list "test-dbi" "mykey" 42 :data :data nil)]
      (is (thrown-with-msg? Exception #"sequential collection"
            (vld/validate-kv-tx-data tx false))))))

;; ---- DB validators ----

(deftest test-validate-schema
  (testing "passes for valid schema"
    (is (nil? (vld/validate-schema
                {:name {:db/valueType :db.type/string}
                 :age  {:db/valueType :db.type/long}}))))

  (testing "raises for bad isComponent without ref"
    (is (thrown-with-msg? Exception #"isComponent"
          (vld/validate-schema
            {:item {:db/isComponent true
                    :db/valueType   :db.type/string}}))))

  (testing "raises for bad cardinality spec"
    (is (thrown-with-msg? Exception #"Bad attribute"
          (vld/validate-schema
            {:name {:db/cardinality :wrong}}))))

  (testing "raises for tuple without tuple props"
    (is (thrown-with-msg? Exception #"tupleAttrs"
          (vld/validate-schema
            {:coord {:db/valueType :db.type/tuple}})))))

(deftest test-validate-attr
  (testing "passes for keywords"
    (is (nil? (vld/validate-attr :name {:entity "test"}))))

  (testing "raises for non-keywords"
    (is (thrown-with-msg? Exception #"Bad entity attribute"
          (vld/validate-attr "name" {:entity "test"})))))

(deftest test-validate-val
  (testing "passes for non-nil"
    (is (nil? (vld/validate-val "hello" {:entity "test"})))
    (is (nil? (vld/validate-val 0 {:entity "test"})))
    (is (nil? (vld/validate-val false {:entity "test"}))))

  (testing "raises for nil"
    (is (thrown-with-msg? Exception #"Cannot store nil"
          (vld/validate-val nil {:entity "test"})))))

(deftest test-validate-datom-unique
  (testing "passes when not unique"
    (is (= "test" (vld/validate-datom-unique
                    false
                    (d/datom 1 :name "test")
                    (constantly nil)))))

  (testing "passes when unique but no conflict found"
    (is (= "test" (vld/validate-datom-unique
                    true
                    (d/datom 1 :name "test")
                    (constantly nil)))))

  (testing "raises when unique and conflict found"
    (is (thrown-with-msg? Exception #"unique constraint"
          (vld/validate-datom-unique
            true
            (d/datom 1 :name "test")
            (constantly true))))))

(deftest test-validate-upserts
  (testing "passes when upserts agree on single entity"
    (let [upserts {:email {"alice@test.com" 1}}]
      (is (= 1 (vld/validate-upserts {:db/id -1} upserts
                                          (fn [x] (and (number? x) (neg? ^long x))))))))

  (testing "raises on conflicting upserts"
    (let [upserts {:email {"alice@test.com" 1}
                   :name  {"Bob" 2}}]
      (is (thrown-with-msg? Exception #"Conflicting upserts"
            (vld/validate-upserts {:db/id -1} upserts
                                       (fn [x] (and (number? x) (neg? ^long x)))))))))

(deftest test-validate-type
  (testing "passes when data matches type"
    (let [dir  (u/tmp-dir (str "vtype-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/string}}
                              {:kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (= :db.type/string (vld/validate-type store :name "hello"))))
        (finally
          (d/close conn)
          (u/delete-files dir)))))

  (testing "raises on mismatch when validate-data? is set"
    (let [dir  (u/tmp-dir (str "vtype-raise-" (UUID/randomUUID)))
          conn (d/create-conn dir {:name {:db/valueType :db.type/long}}
                              {:validate-data? true
                               :kv-opts
                               {:flags (conj c/default-env-flags :nosync)}})]
      (try
        (let [store (:store (d/db conn))]
          (is (thrown-with-msg? Exception #"Invalid data"
                (vld/validate-type store :name "not-a-long"))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-validate-option-mutation
  (testing "boolean options accept true/false"
    (is (nil? (vld/validate-option-mutation :closed-schema? true)))
    (is (nil? (vld/validate-option-mutation :closed-schema? false)))
    (is (nil? (vld/validate-option-mutation :validate-data? true)))
    (is (nil? (vld/validate-option-mutation :auto-entity-time? false)))
    (is (nil? (vld/validate-option-mutation :background-sampling? true)))
    (is (nil? (vld/validate-option-mutation :wal? true)))
    (is (nil? (vld/validate-option-mutation :wal? true)))
    (is (nil? (vld/validate-option-mutation :wal-sync-adaptive? false)))
    (is (nil? (vld/validate-option-mutation :wal-rollback? false))))

  (testing "boolean options reject non-booleans"
    (is (thrown-with-msg? Exception #"expects a boolean"
          (vld/validate-option-mutation :closed-schema? "yes")))
    (is (thrown-with-msg? Exception #"expects a boolean"
          (vld/validate-option-mutation :validate-data? 1)))
    (is (thrown-with-msg? Exception #"expects a boolean"
          (vld/validate-option-mutation :wal-rollback? :rollback)))
    (is (thrown-with-msg? Exception #"expects a boolean"
          (vld/validate-option-mutation :wal? :yes))))

  (testing ":cache-limit accepts non-negative integers"
    (is (nil? (vld/validate-option-mutation :cache-limit 0)))
    (is (nil? (vld/validate-option-mutation :cache-limit 512)))
    (is (nil? (vld/validate-option-mutation :cache-limit 1024)))
    (is (nil? (vld/validate-option-mutation :wal-group-commit 0)))
    (is (nil? (vld/validate-option-mutation :wal-group-commit-ms 4))))

  (testing ":cache-limit rejects non-integers and negatives"
    (is (thrown-with-msg? Exception #"non-negative integer"
          (vld/validate-option-mutation :cache-limit -1)))
    (is (thrown-with-msg? Exception #"non-negative integer"
          (vld/validate-option-mutation :cache-limit "big")))
    (is (thrown-with-msg? Exception #"non-negative integer"
          (vld/validate-option-mutation :cache-limit 3.14)))
    (is (thrown-with-msg? Exception #"non-negative integer"
          (vld/validate-option-mutation :wal-group-commit -1))))

  (testing "wal positive integer options"
    (is (nil? (vld/validate-option-mutation :wal-commit-wait-ms 1)))
    (is (nil? (vld/validate-option-mutation :wal-segment-max-bytes 1024)))
    (is (nil? (vld/validate-option-mutation :wal-vec-checkpoint-interval-ms
                                             300000)))
    (is (nil? (vld/validate-option-mutation :wal-vec-max-lsn-delta
                                             100000)))
    (is (nil? (vld/validate-option-mutation :wal-vec-max-buffer-bytes
                                             1024)))
    (is (nil? (vld/validate-option-mutation :wal-vec-max-buffer-bytes
                                             1024)))
    (is (nil? (vld/validate-option-mutation :wal-vec-chunk-bytes
                                             2048)))
    (is (nil? (vld/validate-option-mutation
               :wal-retention-pin-backpressure-threshold-ms 1)))
    (is (thrown-with-msg? Exception #"positive integer"
          (vld/validate-option-mutation :wal-commit-wait-ms 0)))
    (is (thrown-with-msg? Exception #"positive integer"
          (vld/validate-option-mutation :wal-vec-max-buffer-bytes 0)))
    (is (thrown-with-msg? Exception #"positive integer"
          (vld/validate-option-mutation :wal-retention-ms -10)))
    (is (thrown-with-msg? Exception #"positive integer"
          (vld/validate-option-mutation
           :wal-retention-pin-backpressure-threshold-ms 0))))

  (testing ":db-name accepts strings"
    (is (nil? (vld/validate-option-mutation :db-name "my-db"))))

  (testing ":db-name rejects non-strings"
    (is (thrown-with-msg? Exception #"expects a string"
          (vld/validate-option-mutation :db-name 42))))

  (testing "wal keyword enums"
    (is (nil? (vld/validate-option-mutation :wal-durability-profile :relaxed)))
    (is (nil? (vld/validate-option-mutation :wal-durability-profile :strict)))
    (is (nil? (vld/validate-option-mutation :wal-durability-profile :strict)))
    (is (nil? (vld/validate-option-mutation :wal-sync-mode :fdatasync)))
    (is (nil? (vld/validate-option-mutation :wal-sync-mode :fdatasync)))
    (is (nil? (vld/validate-option-mutation :wal-segment-prealloc-mode :native)))
    (is (nil? (vld/validate-option-mutation :wal-segment-prealloc-mode :mmap)))
    (is (nil? (vld/validate-option-mutation :wal-rollout-mode :rollback)))
    (is (nil? (vld/validate-option-mutation :wal-rollout-mode :rollback)))
    (is (thrown-with-msg? Exception #"expects one of"
          (vld/validate-option-mutation :wal-durability-profile :bad-profile)))
    (is (thrown-with-msg? Exception #"expects one of"
          (vld/validate-option-mutation :wal-durability-profile :bad-profile)))
    (is (thrown-with-msg? Exception #"expects one of"
          (vld/validate-option-mutation :wal-sync-mode :bad-mode)))
    (is (thrown-with-msg? Exception #"expects one of"
          (vld/validate-option-mutation :wal-rollout-mode :bad-mode))))

  (testing "legacy txn-log keys are rejected"
    (is (thrown-with-msg?
          Exception
          #"Legacy txn-log option key is no longer supported"
          (vld/validate-option-mutation :txn-log? true)))
    (is (thrown-with-msg?
          Exception
          #"Legacy txn-log option key is no longer supported"
          (vld/validate-option-mutation
           :txn-log-durability-profile :strict))))

  (testing "unknown options pass through without error"
    (is (nil? (vld/validate-option-mutation :some-custom-opt "anything")))))

;; ---- Schema key validator ----

(deftest test-validate-schema-key
  (testing "passes when value is nil"
    (is (nil? (vld/validate-schema-key :name :db/unique nil
                #{:db.unique/value :db.unique/identity}))))

  (testing "passes when value is in expected set"
    (is (nil? (vld/validate-schema-key :name :db/unique :db.unique/identity
                #{:db.unique/value :db.unique/identity}))))

  (testing "raises when value is not in expected set"
    (is (thrown-with-msg? Exception #"Bad attribute specification"
          (vld/validate-schema-key :name :db/unique :wrong
            #{:db.unique/value :db.unique/identity})))))

;; ---- Transaction form validators ----

(deftest test-validate-tempid-op
  (testing "passes for :db/add with tempid"
    (is (nil? (vld/validate-tempid-op true :db/add [:db/add -1 :name "x"]))))

  (testing "passes for non-tempid regardless of op"
    (is (nil? (vld/validate-tempid-op false :db/retract [:db/retract 1 :name "x"]))))

  (testing "raises for tempid with non-:db/add op"
    (is (thrown-with-msg? Exception #"Tempids are allowed in :db/add only"
          (vld/validate-tempid-op true :db/retract [:db/retract -1 :name "x"])))))

(deftest test-validate-cas-value
  (testing "passes for multival when old value found among datoms"
    (let [datoms [(d/datom 1 :tags "a") (d/datom 1 :tags "b")]]
      (is (nil? (vld/validate-cas-value true 1 :tags "a" "c" datoms)))))

  (testing "raises for multival when old value not found"
    (let [datoms [(d/datom 1 :tags "a") (d/datom 1 :tags "b")]]
      (is (thrown-with-msg? Exception #":db.fn/cas failed"
            (vld/validate-cas-value true 1 :tags "missing" "c" datoms)))))

  (testing "passes for single-val when datom value matches"
    (let [datoms [(d/datom 1 :name "Alice")]]
      (is (nil? (vld/validate-cas-value false 1 :name "Alice" "Bob" datoms)))))

  (testing "raises for single-val when datom value doesn't match"
    (let [datoms [(d/datom 1 :name "Alice")]]
      (is (thrown-with-msg? Exception #":db.fn/cas failed"
            (vld/validate-cas-value false 1 :name "Bob" "Carol" datoms))))))

(deftest test-validate-patch-idoc-arity
  (testing "passes for arity 4"
    (is (nil? (vld/validate-patch-idoc-arity
                4 [:db.fn/patchIdoc 1 :doc {}] :db.fn/patchIdoc))))

  (testing "passes for arity 5"
    (is (nil? (vld/validate-patch-idoc-arity
                5 [:db.fn/patchIdoc 1 :doc {} "old"] :db.fn/patchIdoc))))

  (testing "raises for wrong arity"
    (is (thrown-with-msg? Exception #"Bad arity for :db.fn/patchIdoc"
          (vld/validate-patch-idoc-arity
            3 [:db.fn/patchIdoc 1 :doc] :db.fn/patchIdoc)))
    (is (thrown-with-msg? Exception #"Bad arity for :db.fn/patchIdoc"
          (vld/validate-patch-idoc-arity
            6 [:db.fn/patchIdoc 1 :doc {} "old" "extra"] :db.fn/patchIdoc)))))

(deftest test-validate-patch-idoc-type
  (testing "passes for idoc type"
    (is (nil? (vld/validate-patch-idoc-type :db.type/idoc :doc))))

  (testing "raises for non-idoc type"
    (is (thrown-with-msg? Exception #"not an idoc type"
          (vld/validate-patch-idoc-type :db.type/string :doc)))))

(deftest test-validate-patch-idoc-cardinality
  (testing "passes for cardinality-many with old value"
    (is (nil? (vld/validate-patch-idoc-cardinality true "old-v" :doc))))

  (testing "passes for cardinality-one without old value"
    (is (nil? (vld/validate-patch-idoc-cardinality false nil :doc))))

  (testing "raises for cardinality-many without old value"
    (is (thrown-with-msg? Exception #"requires old value"
          (vld/validate-patch-idoc-cardinality true nil :doc))))

  (testing "raises for cardinality-one with old value"
    (is (thrown-with-msg? Exception #"only supported for cardinality many"
          (vld/validate-patch-idoc-cardinality false "old-v" :doc)))))

(deftest test-validate-patch-idoc-old-value
  (testing "passes when old datom exists"
    (is (nil? (vld/validate-patch-idoc-old-value
                (d/datom 1 :doc "old") "old" :doc))))

  (testing "raises when old datom is nil"
    (is (thrown-with-msg? Exception #"old value not found"
          (vld/validate-patch-idoc-old-value nil "old" :doc)))))

(deftest test-validate-custom-tx-fn-value
  (testing "passes when fun is a function"
    (is (nil? (vld/validate-custom-tx-fn-value
                inc :my-fn [:db.fn/call :my-fn]))))

  (testing "raises when fun is not a function"
    (is (thrown-with-msg? Exception #"expected to have :db/fn"
          (vld/validate-custom-tx-fn-value
            "not-a-fn" :my-fn [:db.fn/call :my-fn])))
    (is (thrown-with-msg? Exception #"expected to have :db/fn"
          (vld/validate-custom-tx-fn-value
            nil :my-fn [:db.fn/call :my-fn])))))

(deftest test-validate-custom-tx-fn-entity
  (testing "passes when ident is non-nil"
    (is (nil? (vld/validate-custom-tx-fn-entity
                {:db/fn inc} :my-fn [:db.fn/call :my-fn]))))

  (testing "raises when ident is nil"
    (is (thrown-with-msg? Exception #"find entity for transaction fn"
          (vld/validate-custom-tx-fn-entity
            nil :my-fn [:db.fn/call :my-fn])))))

(deftest test-validate-tuple-direct-write
  (testing "passes when match? is true"
    (is (nil? (vld/validate-tuple-direct-write true [:db/add 1 :name "x"]))))

  (testing "raises when match? is false"
    (is (thrown-with-msg? Exception #"modify tuple attrs directly"
          (vld/validate-tuple-direct-write false [:db/add 1 :coord [1 2]])))))

(deftest test-validate-tx-op
  (testing "always raises with unknown op"
    (is (thrown-with-msg? Exception #"Unknown operation"
          (vld/validate-tx-op :bogus [:bogus 1 :name "x"])))
    (is (thrown-with-msg? Exception #"Unknown operation"
          (vld/validate-tx-op :db/update [:db/update 1 :name "x"])))))

(deftest test-validate-tx-entity-type
  (testing "always raises with bad entity type"
    (is (thrown-with-msg? Exception #"Bad entity type"
          (vld/validate-tx-entity-type "not-a-valid-entity")))
    (is (thrown-with-msg? Exception #"Bad entity type"
          (vld/validate-tx-entity-type 42)))))

;; ---- Entity-id / lookup-ref validators ----

(deftest test-validate-lookup-ref-shape
  (testing "passes for 2-element lookup ref"
    (is (nil? (vld/validate-lookup-ref-shape [:name "Alice"]))))

  (testing "raises for wrong element count"
    (is (thrown-with-msg? Exception #"Lookup ref should contain 2 elements"
          (vld/validate-lookup-ref-shape [:name])))
    (is (thrown-with-msg? Exception #"Lookup ref should contain 2 elements"
          (vld/validate-lookup-ref-shape [:name "Alice" :extra])))))

(deftest test-validate-lookup-ref-unique
  (testing "passes when unique? is true"
    (is (nil? (vld/validate-lookup-ref-unique true [:name "Alice"]))))

  (testing "raises when unique? is false"
    (is (thrown-with-msg? Exception #"should be marked as :db/unique"
          (vld/validate-lookup-ref-unique false [:name "Alice"])))))

(deftest test-validate-entity-id-syntax
  (testing "always raises"
    (is (thrown-with-msg? Exception #"Expected number or lookup ref"
          (vld/validate-entity-id-syntax "bad-id")))
    (is (thrown-with-msg? Exception #"Expected number or lookup ref"
          (vld/validate-entity-id-syntax :keyword-id)))))

(deftest test-validate-map-entity-id-syntax
  (testing "always raises"
    (is (thrown-with-msg? Exception #"Expected number, string or lookup ref"
          (vld/validate-map-entity-id-syntax :keyword-id)))
    (is (thrown-with-msg? Exception #"Expected number, string or lookup ref"
          (vld/validate-map-entity-id-syntax true)))))

(deftest test-validate-entity-id-exists
  (testing "passes when result is non-nil"
    (is (nil? (vld/validate-entity-id-exists 42 [:name "Alice"]))))

  (testing "raises when result is nil"
    (is (thrown-with-msg? Exception #"Nothing found for entity id"
          (vld/validate-entity-id-exists nil [:name "Alice"])))))

(deftest test-validate-reverse-ref-attr
  (testing "passes for keyword"
    (is (nil? (vld/validate-reverse-ref-attr :_parent))))

  (testing "raises for non-keyword"
    (is (thrown-with-msg? Exception #"Bad entity attribute"
          (vld/validate-reverse-ref-attr "not-a-keyword")))))

(deftest test-validate-reverse-ref-type
  (testing "passes when ref? is true"
    (is (nil? (vld/validate-reverse-ref-type true :parent 1 [2 3]))))

  (testing "raises when ref? is false"
    (is (thrown-with-msg? Exception #"reverse attribute name requires"
          (vld/validate-reverse-ref-type false :parent 1 [2 3])))))

;; ---- Finalize-phase consistency validators ----

(deftest test-validate-value-tempids
  (testing "passes when no unused tempids"
    (is (nil? (vld/validate-value-tempids [])))
    (is (nil? (vld/validate-value-tempids nil))))

  (testing "raises when unused tempids exist"
    (is (thrown-with-msg? Exception #"Tempids used only as value"
          (vld/validate-value-tempids [-1 -2])))))

(deftest test-validate-upsert-retry-conflict
  (testing "passes when eid is nil"
    (is (nil? (vld/validate-upsert-retry-conflict nil -1 42))))

  (testing "raises when eid is non-nil (conflict)"
    (is (thrown-with-msg? Exception #"Conflicting upsert"
          (vld/validate-upsert-retry-conflict 99 -1 42)))))

(deftest test-validate-upsert-conflict
  (testing "passes when tempid is non-nil"
    (is (nil? (vld/validate-upsert-conflict -1 1 42 {:db/id -1}))))

  (testing "raises when tempid is nil"
    (is (thrown-with-msg? Exception #"Conflicting upsert"
          (vld/validate-upsert-conflict nil 1 42 {:db/id 1})))))

(deftest test-validate-tx-data-shape
  (testing "passes for nil"
    (is (nil? (vld/validate-tx-data-shape nil))))

  (testing "passes for sequential collection"
    (is (nil? (vld/validate-tx-data-shape [])))
    (is (nil? (vld/validate-tx-data-shape [[:db/add 1 :name "x"]])))
    (is (nil? (vld/validate-tx-data-shape '([:db/add 1 :name "x"])))))

  (testing "raises for non-sequential"
    (is (thrown-with-msg? Exception #"Bad transaction data"
          (vld/validate-tx-data-shape {:a 1})))
    (is (thrown-with-msg? Exception #"Bad transaction data"
          (vld/validate-tx-data-shape "bad")))))

(deftest test-validate-attr-deletable
  (testing "passes when not populated"
    (is (nil? (vld/validate-attr-deletable false))))

  (testing "raises when populated"
    (is (thrown-with-msg? Exception #"Cannot delete attribute with datoms"
          (vld/validate-attr-deletable true)))))

(deftest test-validate-datom-list
  (testing "passes for empty list"
    (is (nil? (vld/validate-datom-list []))))

  (testing "passes for valid datom list"
    (is (nil? (vld/validate-datom-list
                [(d/datom 1 :name "Alice") (d/datom 2 :name "Bob")]))))

  (testing "raises when non-datom element found"
    (is (thrown-with-msg? Exception #"init-db expects list of Datoms"
          (vld/validate-datom-list [{:name "Alice"}])))
    (is (thrown-with-msg? Exception #"init-db expects list of Datoms"
          (vld/validate-datom-list [(d/datom 1 :name "Alice") "not-a-datom"])))))
