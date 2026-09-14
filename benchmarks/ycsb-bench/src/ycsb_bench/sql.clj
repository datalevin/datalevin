(ns ycsb-bench.sql
  "Prepared JDBC operations for the SQLite and PostgreSQL comparisons."
  (:require [clojure.string :as str]
            [datalevin.util :as u]
            [ycsb-bench.store :as store]
            [ycsb-bench.workload :as w])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.sql Connection DriverManager PreparedStatement ResultSet Statement]
           [java.util Properties UUID]
           [java.util.concurrent ArrayBlockingQueue TimeUnit]))

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
       :transaction-mode :immediate})
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
      (do (.setProperty props "transaction_mode" "IMMEDIATE")
          (.setProperty props "busy_timeout" (str timeout-ms)))
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

(defn- transaction! [^Connection conn f]
  ;; Xerial starts BEGIN IMMEDIATE here, acquiring the writer slot before
  ;; a read. PostgreSQL locks the selected row with SELECT ... FOR UPDATE.
  (.setAutoCommit conn false)
  (try
    (let [result (f)] (.commit conn) result)
    (catch Throwable t
      (try
        (.rollback conn)
        (catch Throwable rollback
          (.addSuppressed t rollback)
          ;; Do not switch an uncertain transaction back to auto-commit.
          (try (.close conn) (catch Throwable close (.addSuppressed t close)))))
      (throw t))
    (finally (when-not (.isClosed conn) (.setAutoCommit conn true)))))

(defn- close-connections! [connections]
  (let [failure (volatile! nil)]
    (doseq [^Connection conn connections]
      (try (.close conn)
           (catch Throwable t
             (if-let [^Throwable first-error @failure]
               (.addSuppressed first-error t)
               (vreset! failure t)))))
    (when-let [t @failure] (throw t))))

(defn- prepare-session [^Connection conn table fields timeout-ms system]
  (let [columns (str/join "," (map #(str "f" %) (range fields)))
        select (str "SELECT " columns " FROM " table " WHERE id=?")
        prepare (fn [^String sql]
                  (let [stmt (.prepareStatement conn sql)]
                    (.setQueryTimeout stmt (int (max 1 (quot (+ (long timeout-ms) 999) 1000))))
                    stmt))]
    ;; Closing the connection also closes any statements allocated before
    ;; an exception during construction. The owner registers it first.
    {:connection conn
     :read (prepare select)
     :read-for-update (prepare (str select (when (= system :postgres) " FOR UPDATE")))
     :scan (prepare (str "SELECT id," columns " FROM " table
                         " WHERE id>=? AND id<? ORDER BY id"))
     :count (prepare (str "SELECT count(*) FROM " table))
     :insert (prepare (str "INSERT INTO " table " (id," columns ") VALUES ("
                           (str/join "," (repeat (inc (long fields)) "?")) ")"))
     :updates (mapv #(prepare (str "UPDATE " table " SET f" % "=? WHERE id=?"))
                     (range fields))}))

(defn- row-values [^ResultSet rs fields offset]
  (mapv #(.getString rs (int (+ (long offset) (long %)))) (range fields)))

(defn- read-row [^PreparedStatement stmt id fields]
  (.setLong stmt 1 (long id))
  (with-open [rs (.executeQuery stmt)]
    (if (.next rs)
      (row-values rs fields 1)
      (throw (ex-info "Missing SQL record" {:id id})))))

(defn- update-row! [session id field value]
  (let [^PreparedStatement stmt (nth (:updates session) field)]
    (.setString stmt 1 value)
    (.setLong stmt 2 (long id))
    (when-not (= 1 (.executeUpdate stmt))
      (throw (ex-info "SQL update did not affect one record" {:id id})))))

(defn- with-session [^ArrayBlockingQueue pool timeout-ms f]
  (if-let [session (.poll pool (long timeout-ms) TimeUnit/MILLISECONDS)]
    (let [failure (volatile! nil)]
      (try
        (f session)
        (catch Throwable t
          (vreset! failure t)
          (throw t))
        (finally
          (try
            ;; A failed rollback closes its connection. Never let another
            ;; worker borrow it; the case owner still retains it for cleanup.
            (when-not (.isClosed ^Connection (:connection session))
              (.add pool session))
            (catch Throwable t
              ;; An uninspectable connection is also discarded, without
              ;; replacing the operation failure with a cleanup error.
              (if-let [^Throwable primary @failure]
                (when-not (identical? primary t) (.addSuppressed primary t))
                (throw t)))))))
    (throw (ex-info "Timed out borrowing a SQL connection" {}))))

(defrecord SQLRecords [pool connections fields timeout-ms info]
  store/Records
  (put-records! [_ records]
    (with-session
      pool timeout-ms
      (fn [{:keys [connection insert]}]
        (let [^PreparedStatement stmt insert]
          (try
            (transaction!
              connection
              (fn []
                (doseq [[id values] records]
                  (.setLong stmt 1 (long id))
                  (doseq [[field value] (map-indexed vector values)]
                    (.setString stmt (+ 2 (long field)) value))
                  (.addBatch stmt))
                (doseq [n (.executeBatch stmt)]
                  (when-not (or (= n 1) (= n Statement/SUCCESS_NO_INFO))
                    (throw (ex-info "SQL insert batch failed" {:update-count n}))))))
            (finally (.clearBatch stmt)))))))
  (read-record [_ id]
    (with-session pool timeout-ms #(read-row (:read %) id fields)))
  (update-field! [_ id field value]
    (with-session pool timeout-ms #(update-row! % id field value)))
  (modify-field! [_ id field]
    (with-session pool timeout-ms
      (fn [session]
        (transaction! (:connection session)
          #(let [values (read-row (:read-for-update session) id fields)]
             (update-row! session id field (w/modified-value (nth values field))))))))
  (scan-records [_ start n]
    (with-session pool timeout-ms
      (fn [session]
        (let [^PreparedStatement stmt (:scan session)]
          (.setLong stmt 1 (long start))
          (.setLong stmt 2 (+ (long start) (long n)))
          (with-open [rs (.executeQuery stmt)]
            (loop [rows (transient [])]
              (if (.next rs)
                (recur (conj! rows [(.getLong rs 1) (row-values rs fields 2)]))
                (persistent! rows))))))))
  (record-count [_]
    (with-session pool timeout-ms
      (fn [session]
        (with-open [rs (.executeQuery ^PreparedStatement (:count session))]
          (.next rs)
          (.getLong rs 1)))))
  (storage-info [_] info)
  (close-store! [_] (close-connections! connections)))

(defn with-store
  "Own a fresh SQLite file or PostgreSQL schema for one benchmark case."
  [{:keys [system field-count pool-size timeout-ms keep-db?] :as opts} f]
  (let [root (when (= system :sqlite)
               (str (Files/createTempDirectory "datalevin-ycsb-sqlite-"
                                               (make-array FileAttribute 0))))
        schema (when (= system :postgres)
                 (str "ycsb_" (str/replace (str (UUID/randomUUID)) "-" "")))
        url (if root (str "jdbc:sqlite:" root "/records.sqlite") (postgres-url opts))
        connections (atom [])
        created? (atom false)]
    (try
      (let [open! (fn [] (let [conn (connect url opts)]
                           (swap! connections conj conn)
                           (configure! conn opts)
                           conn))
            ^Connection first-conn (open!)
            table (if schema (str schema ".records") "records")]
        (when schema
          (execute-sql! first-conn (str "CREATE SCHEMA " schema))
          (reset! created? true))
        (execute-sql! first-conn
                      (str "CREATE TABLE " table " (id "
                           (if (= system :sqlite) "INTEGER" "BIGINT") " PRIMARY KEY,"
                           (str/join "," (map #(str "f" % " TEXT NOT NULL") (range field-count))) ")"))
        (dotimes [_ (dec (long pool-size))] (open!))
        (let [pool (ArrayBlockingQueue. (int pool-size))
              metadata (.getMetaData first-conn)
              info {:layout :sql-row :atomic-rmw? true
                    :client-topology {:handles :pooled :read-connections pool-size
                                      :transaction-connections :same-pool}
                    :engine system :engine-version (.getDatabaseProductVersion metadata)
                    :jdbc-driver-version (.getDriverVersion metadata)
                    :pool-size pool-size :configuration (configure! first-conn opts)}
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
          (doseq [conn @connections]
            (.add pool (prepare-session conn table field-count timeout-ms system)))
          (cond-> (f (->SQLRecords pool @connections field-count timeout-ms info))
            (and keep-db? root) (assoc :database-directory root)
            (and keep-db? schema) (assoc :database-schema schema))))
      (finally
        (try
          ;; Release transactions and table locks before dropping the schema.
          (close-connections! @connections)
          (finally
            (try
              (when (and schema @created? (not keep-db?))
                ;; Use a fresh connection: a failed worker may have lost its
                ;; connection. Only the schema created by this invocation is owned.
                (with-open [conn (connect url opts)]
                  (configure! conn opts)
                  (execute-sql! conn (str "DROP SCHEMA " schema " CASCADE"))))
              (finally (when (and root (not keep-db?)) (u/delete-files root))))))))))
