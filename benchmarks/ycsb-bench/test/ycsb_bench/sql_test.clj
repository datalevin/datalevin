(ns ycsb-bench.sql-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [ycsb-bench.core-test :as shared]
            [ycsb-bench.runner :as runner]
            [ycsb-bench.sql :as sql]
            [ycsb-bench.store :as store])
  (:import [java.sql Connection PreparedStatement ResultSetMetaData SQLException]
           [java.util HashMap]
           [org.postgresql PGResultSetMetaData]))

(defn- systems []
  (cond-> [:sqlite] (System/getenv "YCSB_PG_URL") (conj :postgres)))

(deftest worker-connections-and-lazy-statements-test
  (doseq [system (systems)]
    (sql/with-store
      (assoc shared/small-options :system system :api :kv :threads 2 :pool-size 1)
      (fn [group]
        (let [first-worker (store/for-worker group 0)
              second-worker (store/for-worker group 1)
              first-cache ^HashMap (:statements first-worker)
              second-cache ^HashMap (:statements second-worker)
              values ["aaaa" "bbbb" "cccc"]]
          (is (= 2 (count (:stores group))))
          (is (identical? first-worker (store/for-worker group 0)))
          (is (not (identical? (:connection first-worker) (:connection second-worker))))
          (is (.isEmpty first-cache))
          (is (.isEmpty second-cache))
          (store/put-records! group [["user1" values]])
          (is (= #{:insert} (set (.keySet first-cache))))
          (doseq [worker [first-worker second-worker]]
            (is (.getAutoCommit ^Connection (:connection worker)))
            (is (= values (store/read-record worker "user1"))))
          (let [prepared (.get first-cache :read)]
            (store/read-record first-worker "user1")
            (is (identical? prepared (.get first-cache :read)))
            (is (not (identical? prepared (.get second-cache :read)))))
          (is (= {:handles :per-worker :read-connections 2 :transaction-connections :same-worker}
                 (:client-topology (store/storage-info group)))))
        {}))))

(deftest inserts-use-autocommit-execute-update-test
  (doseq [system (systems), workload [:a :d :e :f], durability [:strict :relaxed]]
    (testing (str [system workload durability])
      (sql/with-store
        (assoc shared/small-options :system system :api :datalog :threads 2
               :workload workload :durability durability)
        (fn [group]
          (let [db (store/for-worker group 0)
                observer (store/for-worker group 1)
                ^PreparedStatement insert (#'sql/statement db :insert)
                calls (atom [])
                prepared (reify PreparedStatement
                           (setString [_ index value] (.setString insert index value))
                           (executeUpdate [_]
                             (is (.getAutoCommit ^Connection (:connection db)))
                             (swap! calls conj :execute-update)
                             (.executeUpdate insert)))
                values ["aaaa" "bbbb" "cccc"]]
            (.put ^HashMap (:statements db) :insert prepared)
            (store/put-records! db [["user1" values] ["user2" values]])
            (is (= [:execute-update :execute-update] @calls))
            (is (= values (store/read-record observer "user1")))
            (is (= values (store/read-record observer "user2")))
            ;; A later duplicate does not roll back an earlier committed row.
            (is (thrown? SQLException
                         (store/put-records! db [["user3" values] ["user1" ["xxxx" "yyyy" "zzzz"]]])))
            (is (= values (store/read-record observer "user3")))
            (is (= values (store/read-record observer "user1")))
            (is (= 3 (store/record-count observer)))
            (is (.getAutoCommit ^Connection (:connection db))))
          {})))))

(deftest upstream-schema-and-select-test
  (doseq [system (systems), workload [:a :b :c :d :e :f]]
    (sql/with-store
      (assoc shared/small-options :system system :api :kv :workload workload)
      (fn [group]
        (let [db (store/for-worker group 0)
              stmt (#'sql/statement db :read)
              metadata (.getMetaData stmt)]
          (is (= 4 (.getColumnCount metadata)) "SELECT * includes the string primary key")
          (is (= ["YCSB_KEY" "FIELD0" "FIELD1" "FIELD2"]
                 (mapv #(str/upper-case (.getColumnName metadata (int %))) (range 1 5))))
          (is (= "VARCHAR" (str/upper-case (.getColumnTypeName metadata 1))))
          (doseq [column (range 2 5)]
            (is (= ResultSetMetaData/columnNullable (.isNullable metadata (int column)))))
          (when (= system :sqlite)
            (let [^Connection conn (:connection db)
                  ddl (#'sql/scalar conn "SELECT sql FROM sqlite_master WHERE name='records'")]
              (is (not (str/includes? ddl "WITHOUT ROWID")))
              (is (not (str/includes? ddl "COLLATE")))
              (store/put-records! db [["user10" ["aaaa" "bbbb" "cccc"]]])
              (is (= "text" (#'sql/scalar conn "SELECT typeof(YCSB_KEY) FROM records")))
              (is (= "1" (#'sql/scalar conn "SELECT rowid FROM records"))))))
        {}))))

(defn- field-indexes [db]
  (let [db (store/for-worker db 0)
        ^Connection conn (:connection db)
        ^PreparedStatement read-statement (#'sql/statement db :read)
        schema (when (= :postgres (:engine (store/storage-info db)))
                 (.getBaseSchemaName ^PGResultSetMetaData (.getMetaData read-statement) 1))]
    (with-open [rs (.getIndexInfo (.getMetaData conn) nil schema "records" false false)]
      (loop [indexes #{}]
        (if (.next rs)
          (let [column (.getString rs "COLUMN_NAME")]
            (recur (if (or (nil? column) (= "ycsb_key" (str/lower-case column)))
                     indexes
                     (conj indexes {:name (.getString rs "INDEX_NAME")
                                    :column column
                                    :non-unique? (.getBoolean rs "NON_UNIQUE")
                                    :position (.getShort rs "ORDINAL_POSITION")}))))
          indexes)))))

(defn- check-field-indexes! [db]
  (is (empty? (field-indexes db)) "Upstream SQL has no payload indexes")
  (is (= :none (:payload-indexes (store/storage-info db))))
  (is (= [] (get-in (store/storage-info db) [:configuration :secondary-indexes]))
      "Reports describe the indexes maintained during the benchmark"))

(defn- check-sql! [opts]
  (sql/with-store
    opts
    (fn [db]
      (check-field-indexes! db)
      (let [values ["aaaa" "bbbb" "cccc"]]
        (store/put-records! db [["user2" values] ["user0" values] ["user10" values]])
        (is (= values (store/read-record db "user10")))
        (store/update-field! db "user10" 0 "xxxx")
        (store/update-field! db "user10" 2 "zzzz")
        (is (= ["xxxx" "bbbb" "zzzz"] (store/read-record db "user10")))
        (is (= [["user10" ["xxxx" "bbbb" "zzzz"]] ["user2" values]]
               (store/scan-records db "user1" 2)))
        (is (= [["user2" values]] (store/scan-records db "user11" 10)))
        (is (empty? (store/scan-records db "user3" 1)))
        (is (empty? (store/scan-records db "user0" 0)))
        (is (thrown? clojure.lang.ExceptionInfo (store/read-record db "absent")))
        (is (= 3 (store/record-count db))))
      {:storage (store/storage-info db)})))

(deftest unindexed-payload-field-count-test
  (doseq [system (systems)
          api [:kv :datalog]
          field-count [1 10]]
    (testing (str system " " api " with " field-count " value columns")
      (sql/with-store
        (assoc shared/small-options :system system :api api :field-count field-count)
        (fn [db]
          (check-field-indexes! db)
          {})))))

(deftest sqlite-semantics-and-durability-test
  (doseq [durability [:strict :relaxed]]
    (let [result (check-sql! (assoc shared/small-options :system :sqlite
                                   :api :datalog :mode :embedded :durability durability))]
      (is (= "wal" (get-in result [:storage :configuration :journal-mode])))
      (is (= (if (= durability :strict) "2" "1")
             (get-in result [:storage :configuration :synchronous]))))))

(deftest postgres-semantics-and-durability-test
  (if (System/getenv "YCSB_PG_URL")
    (doseq [durability [:strict :relaxed]]
      (let [result (check-sql! (assoc shared/small-options :system :postgres
                                     :api :datalog :mode :remote :durability durability))]
        (is (= "on" (get-in result [:storage :configuration :fsync])))
        (is (= "on" (get-in result [:storage :configuration :full-page-writes])))
        (is (= (if (= durability :strict) "on" "off")
               (get-in result [:storage :configuration :synchronous-commit])))))
    (println "Skipping PostgreSQL integration test: set YCSB_PG_URL and optional YCSB_PG_USER/PASSWORD.")))

(deftest sqlite-load-warmup-and-report-test
  (let [result (runner/run-case! (assoc shared/small-options :system :sqlite
                                       :api :datalog :mode :embedded :workload :d
                                       :ops 200 :warmup 50
                                       :pg-url "not-used-secret" :pg-user "not-reported"))
        inserted (get-in result [:measured :by-operation :insert :count] 0)]
    (is (pos? inserted))
    (is (= (+ 4 inserted) (get-in result [:validation :records])))
    (is (= 200 (get-in result [:measured :operations])))
    (is (not (contains? (:configuration result) :pg-url)))
    (is (not (contains? (:configuration result) :pg-user)))))

(deftest failure-closes-all-sql-connections-test
  (doseq [system (cond-> [:sqlite] (System/getenv "YCSB_PG_URL") (conj :postgres))]
    (let [connections (atom [])]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Injected benchmark failure"
            (sql/with-store (assoc shared/small-options :system system :api :datalog)
              (fn [db]
                (reset! connections (mapv :connection (:stores db)))
                (throw (ex-info "Injected benchmark failure" {}))))))
      (is (= 3 (count @connections)))
      (is (every? #(.isClosed ^Connection %) @connections)))))

(deftest cleanup-preserves-primary-failure-test
  (doseq [failed? [false true]]
    (let [primary (when failed? (ex-info "Benchmark failed" {}))
          close-error (ex-info "Close failed" {})
          drop-error (ex-info "Drop failed" {})
          calls (atom [])
          result (try
                   (#'sql/cleanup!
                     primary
                     [#(do (swap! calls conj :close) (throw close-error))
                      #(do (swap! calls conj :drop) (throw drop-error))
                      #(swap! calls conj :files)])
                   (catch Exception e e))]
      (is (= [:close :drop :files] @calls))
      (if primary
        (do
          (is (nil? result))
          (is (= [close-error drop-error] (vec (.getSuppressed primary)))))
        (do
          (is (identical? close-error result))
          (is (= [drop-error] (vec (.getSuppressed close-error)))))))))
