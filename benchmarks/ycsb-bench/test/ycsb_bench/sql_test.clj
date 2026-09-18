(ns ycsb-bench.sql-test
  (:require [clojure.test :refer [deftest is testing]]
            [ycsb-bench.core-test :as shared]
            [ycsb-bench.runner :as runner]
            [ycsb-bench.sql :as sql]
            [ycsb-bench.store :as store])
  (:import [java.sql Connection PreparedStatement SQLException]
           [java.util.concurrent ArrayBlockingQueue]
           [org.postgresql PGResultSetMetaData]))

(deftest rollback-failure-discards-closed-session-test
  (let [failure (SQLException. "Operation failed")
        rollback (SQLException. "Rollback failed")
        closed? (atom false)
        auto-commit (atom [])
        broken (reify Connection
                 (setAutoCommit [_ enabled] (swap! auto-commit conj enabled))
                 (rollback [_] (throw rollback))
                 (close [_] (reset! closed? true))
                 (isClosed [_] @closed?))
        healthy {:connection (reify Connection (isClosed [_] false))}
        pool (ArrayBlockingQueue. 2)]
    (.add pool {:connection broken})
    (.add pool healthy)
    (is (identical? failure
                    (try
                      (#'sql/with-session pool 10
                        (fn [{:keys [connection]}]
                          (#'sql/transaction! connection #(throw failure))))
                      (catch Throwable t t))))
    (is (= [rollback] (vec (.getSuppressed failure))))
    (is @closed?)
    (is (= [false] @auto-commit)
        "An uncertain transaction must not be switched back to auto-commit")
    (is (= [healthy] (vec (.toArray pool)))
        "Only the healthy connection is available to subsequent workers")
    (dotimes [_ 2]
      (is (identical? healthy (#'sql/with-session pool 10 identity))))
    (is (= [healthy] (vec (.toArray pool))))))

(deftest session-state-check-failure-test
  (doseq [operation-fails? [false true]]
    (let [failure (SQLException. "Operation failed")
          inspection (SQLException. "Connection state unavailable")
          pool (ArrayBlockingQueue. 1)]
      (.add pool {:connection (reify Connection (isClosed [_] (throw inspection)))})
      (is (identical? (if operation-fails? failure inspection)
                      (try
                        (#'sql/with-session pool 10
                          (fn [_] (when operation-fails? (throw failure))))
                        (catch Throwable t t))))
      (when operation-fails?
        (is (= [inspection] (vec (.getSuppressed failure)))))
      (is (.isEmpty pool) "A session with unknown state must not be reused"))))

(defn- field-indexes [db]
  (let [^Connection conn (first (:connections db))
        ^PreparedStatement read-statement (:read (.peek ^ArrayBlockingQueue (:pool db)))
        schema (when (= :postgres (:engine (store/storage-info db)))
                 (.getBaseSchemaName ^PGResultSetMetaData (.getMetaData read-statement) 1))]
    (with-open [rs (.getIndexInfo (.getMetaData conn) nil schema "records" false false)]
      (loop [indexes #{}]
        (if (.next rs)
          (let [column (.getString rs "COLUMN_NAME")]
            (recur (if (or (nil? column) (= "id" column))
                     indexes
                     (conj indexes {:name (.getString rs "INDEX_NAME")
                                    :column column
                                    :non-unique? (.getBoolean rs "NON_UNIQUE")
                                    :position (.getShort rs "ORDINAL_POSITION")}))))
          indexes)))))

(defn- check-field-indexes! [db field-count mode]
  (let [expected (mapv (fn [field]
                        {:name (str "records_f" field "_idx") :column (str "f" field)})
                      (if (= mode :all) (range field-count) []))]
    (is (= (set (map #(assoc % :non-unique? true :position 1) expected))
           (field-indexes db))
        "The database has exactly the value indexes selected for this condition")
    (is (= mode (get-in (store/storage-info db) [:configuration :sql-indexes])))
    (is (= expected (get-in (store/storage-info db) [:configuration :secondary-indexes]))
        "Reports describe the indexes maintained during the benchmark")))

(defn- check-sql! [opts]
  (sql/with-store
    opts
    (fn [db]
      (check-field-indexes! db (:field-count opts) (:sql-indexes opts))
      (shared/check-adapter! db)
      (testing "A failed insert batch rolls back completely and releases its connection"
        (is (thrown? SQLException
                     (store/put-records! db [[4 ["dddd" "eeee" "ffff"]]
                                            [0 ["aaaa" "bbbb" "cccc"]]])))
        (is (= 4 (store/record-count db)))
        (is (= [] (store/scan-records db 4 1)))
        (store/update-field! db 1 0 "xxxx")
        (is (= ["xxxx" "bbbb" "cccc"] (store/read-record db 1))))
      {:storage (store/storage-info db)})))

(deftest value-index-field-count-test
  (doseq [system (cond-> [:sqlite] (System/getenv "YCSB_PG_URL") (conj :postgres))
          [api mode] [[:kv :none] [:datalog :all]]
          field-count [1 10]]
    (testing (str system " " api " with " field-count " value columns")
      (sql/with-store
        (assoc shared/small-options :system system :api api :field-count field-count)
        (fn [db]
          (check-field-indexes! db field-count mode)
          {})))))

(deftest sqlite-semantics-and-durability-test
  (doseq [durability [:strict :relaxed], sql-indexes [:none :all]]
    (let [result (check-sql! (assoc shared/small-options :system :sqlite
                                   :api :datalog :mode :embedded :durability durability
                                   :sql-indexes sql-indexes))]
      (is (= "wal" (get-in result [:storage :configuration :journal-mode])))
      (is (= (if (= durability :strict) "2" "1")
             (get-in result [:storage :configuration :synchronous]))))))

(deftest postgres-semantics-and-durability-test
  (if (System/getenv "YCSB_PG_URL")
    (doseq [durability [:strict :relaxed], sql-indexes [:none :all]]
      (let [result (check-sql! (assoc shared/small-options :system :postgres
                                     :api :datalog :mode :remote :durability durability
                                     :sql-indexes sql-indexes))]
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
        inserted (+ (get-in result [:warmup :by-operation :insert :count] 0)
                    (get-in result [:measured :by-operation :insert :count] 0))]
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
                (reset! connections (:connections db))
                (throw (ex-info "Injected benchmark failure" {}))))))
      (is (= 3 (count @connections)))
      (is (every? #(.isClosed ^Connection %) @connections)))))
