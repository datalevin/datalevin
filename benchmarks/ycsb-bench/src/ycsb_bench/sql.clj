(ns ycsb-bench.sql
  "Prepared JDBC operations for the SQLite and PostgreSQL comparisons."
  (:require [clojure.string :as str]
            [datalevin.util :as u]
            [ycsb-bench.store :as store]
            [ycsb-bench.workload :as w])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.sql Connection DriverManager PreparedStatement ResultSet]
           [java.util HashMap Properties UUID]))

(set! *warn-on-reflection* true)

(defn- execute-sql! [^Connection conn ^String sql]
  (with-open [stmt (.createStatement conn)] (.execute stmt sql)))

(defn- scalar [^Connection conn ^String sql]
  (with-open [stmt (.createStatement conn), rs (.executeQuery stmt sql)]
    (.next rs)
    (.getString rs 1)))

(defn- setting! [label expected actual]
  (when-not (= expected actual)
    (throw (ex-info (str "SQL setting mismatch: " label)
                    {:expected expected :actual actual})))
  actual)

(defn- configure! [^Connection conn {:keys [system durability timeout-ms]}]
  (if (= system :sqlite)
    (do
      (setting! :journal-mode "wal" (scalar conn "PRAGMA journal_mode=WAL"))
      (execute-sql! conn (str "PRAGMA synchronous=" (if (= durability :strict) "FULL" "NORMAL")))
      (execute-sql! conn (str "PRAGMA busy_timeout=" timeout-ms))
      {:journal-mode "wal"
       :synchronous (setting! :synchronous (if (= durability :strict) "2" "1")
                               (scalar conn "PRAGMA synchronous"))
       :busy-timeout-ms (parse-long (scalar conn "PRAGMA busy_timeout"))
       :fullfsync (scalar conn "PRAGMA fullfsync")
       :auto-commit (.getAutoCommit conn)})
    (let [sync (if (= durability :strict) "on" "off")]
      (.setTransactionIsolation conn Connection/TRANSACTION_READ_COMMITTED)
      (execute-sql! conn (str "SET synchronous_commit=" sync))
      (execute-sql! conn (str "SET statement_timeout=" timeout-ms))
      (execute-sql! conn (str "SET lock_timeout=" timeout-ms))
      {:synchronous-commit (setting! :synchronous-commit sync (scalar conn "SHOW synchronous_commit"))
       :fsync (setting! :fsync "on" (scalar conn "SHOW fsync"))
       :full-page-writes (setting! :full-page-writes "on" (scalar conn "SHOW full_page_writes"))
       :transaction-isolation (scalar conn "SHOW transaction_isolation")
       :statement-timeout (scalar conn "SHOW statement_timeout")
       :lock-timeout (scalar conn "SHOW lock_timeout")})))

(defn- connect ^Connection [url {:keys [system timeout-ms pg-user]}]
  (let [props (Properties.)]
    (if (= system :sqlite)
      (.setProperty props "busy_timeout" (str timeout-ms))
      (do
        (when-let [user (or pg-user (System/getenv "YCSB_PG_USER"))]
          (.setProperty props "user" user))
        (when-let [password (System/getenv "YCSB_PG_PASSWORD")]
          (.setProperty props "password" password))
        (let [seconds (str (max 1 (quot (+ (long timeout-ms) 999) 1000)))]
          (.setProperty props "connectTimeout" seconds)
          (.setProperty props "socketTimeout" seconds))
        (.setProperty props "ApplicationName" "datalevin-ycsb-bench")))
    (DriverManager/getConnection ^String url props)))

(defn- postgres-url [opts]
  (let [url (or (:pg-url opts) (System/getenv "YCSB_PG_URL")
                "jdbc:postgresql://127.0.0.1:5432/postgres")]
    (when-not (str/starts-with? url "jdbc:postgresql://")
      (throw (ex-info "PostgreSQL comparison requires a TCP JDBC URL" {})))
    url))

(defn check-postgres!
  "Check connectivity and durability before starting a comparison batch."
  [opts]
  (let [opts (assoc opts :system :postgres)]
    (with-open [conn (connect (postgres-url opts) opts)] (configure! conn opts))))

(defn- cleanup!
  "Attempt every cleanup, preserving the original benchmark failure."
  [primary actions]
  (let [failure (reduce (fn [^Throwable failure action]
                          (try
                            (action)
                            failure
                            (catch Throwable t
                              (if failure
                                (do
                                  (when-not (identical? failure t)
                                    (.addSuppressed failure t))
                                  failure)
                                t))))
                        primary actions)]
    (when (and failure (nil? primary)) (throw failure))))

(def upstream-revision "66302f301b13f60d4bcb2f29f478586bb1d6f2e0")

(defn- statement-sql [{:keys [table fields system]} operation]
  (let [columns (str/join "," fields)]
    (case (if (vector? operation) (first operation) operation)
      :read (str "SELECT * FROM " table " WHERE YCSB_KEY = ?")
      :scan (str "SELECT * FROM " table " WHERE YCSB_KEY >= ? ORDER BY YCSB_KEY"
                 (if (= system :postgres) " FETCH FIRST ? ROWS ONLY" " LIMIT ?"))
      :insert (str "INSERT INTO " table " (YCSB_KEY," columns ") VALUES(?"
                   (apply str (repeat (count fields) ",?")) ")")
      :update (str "UPDATE " table " SET " (nth fields (second operation)) "=? WHERE YCSB_KEY = ?")
      :count (str "SELECT count(*) FROM " table))))

(defn- statement ^PreparedStatement
  [{:keys [connection statements] :as db} operation]
  ;; Each worker owns its connection and lazily cached statements, as in
  ;; JdbcDBClient. The load/validation thread uses worker zero between phases.
  (let [^HashMap statements statements]
    (or (.get statements operation)
        (let [stmt (.prepareStatement ^Connection connection (statement-sql db operation))]
          (.put statements operation stmt)
          stmt))))

(defn- row-values [^ResultSet rs fields]
  (mapv #(.getString rs ^String %) fields))

(defrecord SQLRecords [connection statements table fields system info]
  store/Records
  (put-records! [db records]
    (let [stmt (statement db :insert)]
      ;; Upstream defaults: jdbc.autocommit=true, jdbc.batchupdateapi=false.
      ;; Each record commits independently, including during initial loading.
      (doseq [[key values] records]
        (.setString stmt 1 key)
        (doseq [[field value] (map-indexed vector values)]
          (.setString stmt (+ 2 (long field)) value))
        (when-not (= 1 (.executeUpdate stmt))
          (throw (ex-info "SQL insert did not affect one record" {:key key}))))))
  (read-record [db key]
    (let [stmt (statement db :read)]
      (.setString stmt 1 key)
      (with-open [rs (.executeQuery stmt)]
        (if (.next rs)
          (row-values rs fields)
          (throw (ex-info "Missing SQL record" {:key key}))))))
  (update-field! [db key field value]
    (let [stmt (statement db [:update field])]
      (.setString stmt 1 value)
      (.setString stmt 2 key)
      (when-not (= 1 (.executeUpdate stmt))
        (throw (ex-info "SQL update did not affect one record" {:key key})))))
  (scan-records [db start n]
    (if (pos? (long n))
      (let [stmt (statement db :scan)]
        (.setString stmt 1 start)
        (.setInt stmt 2 (int n))
        (with-open [rs (.executeQuery stmt)]
          (loop [rows (transient [])]
            (if (and (< (count rows) (long n)) (.next rs))
              (recur (conj! rows [(.getString rs "YCSB_KEY") (row-values rs fields)]))
              (persistent! rows)))))
      []))
  (record-count [db]
    (with-open [rs (.executeQuery (statement db :count))]
      (.next rs)
      (.getLong rs 1)))
  (storage-info [_] info)
  (close-store! [_] (.close ^Connection connection)))

(defn with-store
  "Own a fresh SQLite file or PostgreSQL schema for one benchmark case."
  [{:keys [system field-count threads keep-db? workload] :as opts} f]
  (let [root (when (= system :sqlite)
               (str (Files/createTempDirectory "datalevin-ycsb-sqlite-"
                                              (make-array FileAttribute 0))))
        schema (when (= system :postgres)
                 (str "ycsb_" (str/replace (str (UUID/randomUUID)) "-" "")))
        url (if root (str "jdbc:sqlite:" root "/records.sqlite") (postgres-url opts))
        connections (atom [])
        created? (atom false)
        failure (volatile! nil)]
    (try
      (let [open! (fn [] (let [conn (connect url opts)]
                           (swap! connections conj conn)
                           (.setAutoCommit ^Connection conn true)
                           [conn (configure! conn opts)]))
            [^Connection first-conn configuration] (open!)
            table (if schema (str schema ".records") "records")]
        (when schema
          (execute-sql! first-conn (str "CREATE SCHEMA " schema))
          (reset! created? true))
        (execute-sql!
          first-conn
          (str "CREATE TABLE " table " (YCSB_KEY VARCHAR(255) PRIMARY KEY,"
               (str/join "," (map #(str "field" % " TEXT") (range field-count))) ")"))
        (dotimes [_ (dec (long threads))] (open!))
        (let [metadata (.getMetaData first-conn)
              info {:layout :sql-row :atomic-rmw? false :rmw-execution :client-read-update
                    :workload-model (w/workload-model workload)
                    :binding-model :upstream-jdbc-v1 :upstream-revision upstream-revision
                    :record-key :YCSB_KEY :key-type :string :key-generator :ycsb-fnv64-decimal
                    :insert-semantics :reject-duplicates :insert-transaction :per-record
                    :payload-indexes :none
                    :read-fields :explicit-all :key-collation :database-default
                    :without-rowid? false
                    :scan-api :prepared-select :scan-selection :attribute-value-range
                    :scan-order :key :scan-start :inclusive
                    :client-topology {:handles :per-worker :read-connections threads
                                      :transaction-connections :same-worker}
                    :engine system :engine-version (.getDatabaseProductVersion metadata)
                    :jdbc-driver-version (.getDriverVersion metadata)
                    :connection-count threads
                    :configuration (assoc configuration
                                          :auto-commit true :batch-update-api false
                                          :secondary-indexes [])}
              info (cond-> info
                     (= system :postgres)
                     (assoc :server
                            {:placement :separate-process
                             :transport :tcp
                             :host (scalar first-conn "SELECT inet_server_addr()::text")
                             :port (parse-long (scalar first-conn "SELECT inet_server_port()"))
                             :configuration
                             (into {}
                                   (map (fn [setting]
                                          [(keyword (str/replace setting "_" "-"))
                                           (scalar first-conn (str "SHOW " setting))]))
                                   ["shared_buffers" "work_mem" "maintenance_work_mem"
                                    "max_connections" "wal_sync_method" "checkpoint_timeout"
                                    "max_wal_size" "autovacuum" "max_parallel_workers_per_gather"])}))]
          (cond-> (f (store/->StoreGroup
                       (mapv #(->SQLRecords % (HashMap.) table
                                             (mapv (fn [n] (str "field" n)) (range field-count))
                                             system info)
                             @connections)
                       info))
            (and keep-db? root) (assoc :database-directory root)
            (and keep-db? schema) (assoc :database-schema schema))))
      (catch Throwable t (vreset! failure t) (throw t))
      (finally
        (cleanup!
          @failure
          (concat
            ;; Release transactions and table locks before dropping the schema.
            (map (fn [^Connection conn] #(.close conn)) @connections)
            [(fn []
               (when (and schema @created? (not keep-db?))
                 ;; A failed worker may have lost its connection. Drop only
                 ;; the schema created by this invocation, using a fresh one.
                 (with-open [conn (connect url opts)]
                   (configure! conn opts)
                   (execute-sql! conn (str "DROP SCHEMA " schema " CASCADE")))))
             #(when (and root (not keep-db?)) (u/delete-files root))]))))))

(defn with-stores
  "Use a fresh SQLite file or PostgreSQL schema for each phase callback.
  JVM/JDBC and the PostgreSQL server stay running between phases."
  [opts f]
  (f (fn [callback] (with-store opts callback))))
