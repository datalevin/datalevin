(ns openrulebench.postgresql
  "PostgreSQL benchmarks for OpenRuleBench using recursive CTEs via JDBC.

   Requires: PostgreSQL running locally.
   Connection: jdbc:postgresql://localhost:5432/postgres"
  (:require
   [openrulebench.core :as core]
   [openrulebench.data :as data])
  (:import
   [java.sql DriverManager Connection]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def db-url "jdbc:postgresql://localhost:5432/postgres")
(def db-user (or (System/getProperty "pg.user") (System/getenv "USER") "postgres"))
(def db-pass (or (System/getProperty "pg.pass") ""))

;; =============================================================================
;; SQL Queries (recursive CTEs)
;; =============================================================================

;; Transitive Closure using recursive CTE
(def tc-query
  "WITH RECURSIVE tc(a, b) AS (
     SELECT a, b FROM edge
     UNION
     SELECT edge.a, tc.b FROM edge, tc WHERE edge.b = tc.a
   )
   SELECT a, b FROM tc;")

;; Same Generation using recursive CTE
(def sg-query
  "WITH RECURSIVE sg(x, y) AS (
     SELECT a, b FROM sib
     UNION
     SELECT DISTINCT p1.a, p2.a
     FROM par p1, sg, par p2
     WHERE p1.b = sg.x AND p2.b = sg.y
   )
   SELECT x, y FROM sg;")

(def join1-query-prefix
  "WITH
     c1(x, y) AS (
       SELECT DISTINCT d1.a, d2.b FROM d1 JOIN d2 ON d1.b = d2.a
     ),
     b2(x, y) AS (
       SELECT DISTINCT c3.a, c4.b FROM c3 JOIN c4 ON c3.b = c4.a
     ),
     b1(x, y) AS (
       SELECT DISTINCT c1.x, c2.b FROM c1 JOIN c2 ON c1.y = c2.a
     ),
     a(x, y) AS (
       SELECT DISTINCT b1.x, b2.y FROM b1 JOIN b2 ON b1.y = b2.x
     )")

(defn- bound-select
  [relation binding bound]
  (case binding
    :ff (format "SELECT x, y FROM %s" relation)
    :bf (format "SELECT y FROM %s WHERE x = %d" relation bound)
    :fb (format "SELECT x FROM %s WHERE y = %d" relation bound)))

(defn tc-query-for
  [binding bound]
  (case binding
    :ff "WITH RECURSIVE tc(a, b) AS (
           SELECT a, b FROM edge
           UNION
           SELECT edge.a, tc.b FROM edge JOIN tc ON edge.b = tc.a
         ) SELECT a, b FROM tc"
    :bf (format "WITH RECURSIVE tc(b) AS (
                   SELECT b FROM edge WHERE a = %d
                   UNION
                   SELECT edge.b FROM tc JOIN edge ON edge.a = tc.b
                 ) SELECT b FROM tc" bound)
    :fb (format "WITH RECURSIVE tc(a) AS (
                   SELECT a FROM edge WHERE b = %d
                   UNION
                   SELECT edge.a FROM tc JOIN edge ON edge.b = tc.a
                 ) SELECT a FROM tc" bound)))

(defn sg-query-for
  [binding bound]
  (case binding
    :ff "WITH RECURSIVE sg(x, y) AS (
           SELECT a, b FROM sib
           UNION
           SELECT p1.a, p2.a
           FROM par p1 JOIN sg ON p1.b = sg.x
                       JOIN par p2 ON p2.b = sg.y
         ) SELECT x, y FROM sg"
    :bf (format "WITH RECURSIVE
                   magic(x) AS (
                     SELECT %d
                     UNION
                     SELECT par.b
                     FROM magic JOIN par ON par.a = magic.x
                   ),
                   sg(x, y) AS (
                     SELECT sib.a, sib.b
                     FROM sib JOIN magic ON magic.x = sib.a
                     UNION
                     SELECT p1.a, p2.a
                     FROM par p1 JOIN magic ON magic.x = p1.a
                                 JOIN sg ON p1.b = sg.x
                                 JOIN par p2 ON p2.b = sg.y
                   )
                 SELECT y FROM sg WHERE x = %d" bound bound)
    :fb (format "WITH RECURSIVE
                   magic(y) AS (
                     SELECT %d
                     UNION
                     SELECT par.b
                     FROM magic JOIN par ON par.a = magic.y
                   ),
                   sg(x, y) AS (
                     SELECT sib.a, sib.b
                     FROM sib JOIN magic ON magic.y = sib.b
                     UNION
                     SELECT p1.a, p2.a
                     FROM par p2 JOIN magic ON magic.y = p2.a
                                 JOIN sg ON p2.b = sg.y
                                 JOIN par p1 ON p1.b = sg.x
                   )
                 SELECT x FROM sg WHERE y = %d" bound bound)))

(defn join1-query-for
  [query binding bound]
  (str join1-query-prefix " " (bound-select (name query) binding bound)))

;; =============================================================================
;; Database Setup
;; =============================================================================

(defn get-connection ^Connection []
  (DriverManager/getConnection db-url db-user db-pass))

(defn execute!
  "Execute a SQL statement."
  [^Connection conn sql]
  (let [stmt (.createStatement conn)]
    (.executeUpdate stmt sql)
    (.close stmt)))

(defn query-rows
  "Run a query and fully materialize every result column."
  [^Connection conn sql]
  (let [stmt (.createStatement conn)
        rs (.executeQuery stmt sql)
        columns (.getColumnCount (.getMetaData rs))]
    (loop [acc (transient [])]
      (if (.next rs)
        (recur (conj! acc
                      (mapv (fn [column] (.getLong rs (int column)))
                            (range 1 (inc columns)))))
        (let [rows (persistent! acc)]
          (.close rs)
          (.close stmt)
          rows)))))

(defn setup-tc-table!
  "Create and populate edge table for TC benchmark."
  [^Connection conn edges]
  (execute! conn (str "CREATE TEMP TABLE edge "
                      "(a INTEGER NOT NULL, b INTEGER NOT NULL, "
                      "PRIMARY KEY (a, b)) ON COMMIT PRESERVE ROWS"))
  ;; Batch insert
  (let [ps (.prepareStatement conn "INSERT INTO edge VALUES (?, ?)")]
    (doseq [[a b] edges]
      (.setLong ps 1 a)
      (.setLong ps 2 b)
      (.addBatch ps))
    (.executeBatch ps)
    (.close ps))
  ;; Create indexes and analyze
  (execute! conn "CREATE INDEX idx_edge_a ON edge(a)")
  (execute! conn "CREATE INDEX idx_edge_b ON edge(b)")
  (execute! conn "ANALYZE edge"))

(defn setup-sg-table!
  "Create and populate par/sib tables for SG benchmark."
  [^Connection conn {:keys [par sib]}]
  (execute! conn (str "CREATE TEMP TABLE par "
                      "(a INTEGER NOT NULL, b INTEGER NOT NULL, "
                      "PRIMARY KEY (a, b)) ON COMMIT PRESERVE ROWS"))
  (execute! conn (str "CREATE TEMP TABLE sib "
                      "(a INTEGER NOT NULL, b INTEGER NOT NULL, "
                      "PRIMARY KEY (a, b)) ON COMMIT PRESERVE ROWS"))
  ;; Batch insert
  (let [ps (.prepareStatement conn "INSERT INTO par VALUES (?, ?)")]
    (doseq [[a b] par]
      (.setLong ps 1 a)
      (.setLong ps 2 b)
      (.addBatch ps))
    (.executeBatch ps)
    (.close ps))
  (let [ps (.prepareStatement conn "INSERT INTO sib VALUES (?, ?)")]
    (doseq [[a b] sib]
      (.setLong ps 1 a)
      (.setLong ps 2 b)
      (.addBatch ps))
    (.executeBatch ps)
    (.close ps))
  ;; Create indexes and analyze
  (execute! conn "CREATE INDEX idx_par_a ON par(a)")
  (execute! conn "CREATE INDEX idx_par_b ON par(b)")
  (execute! conn "CREATE INDEX idx_sib_a ON sib(a)")
  (execute! conn "CREATE INDEX idx_sib_b ON sib(b)")
  (execute! conn "ANALYZE par")
  (execute! conn "ANALYZE sib"))

(defn- setup-relation!
  [^Connection conn table pairs]
  (execute! conn
            (format (str "CREATE TEMP TABLE %s "
                         "(a INTEGER NOT NULL, b INTEGER NOT NULL, "
                         "PRIMARY KEY (a, b)) ON COMMIT PRESERVE ROWS")
                    table))
  (let [ps (.prepareStatement conn (format "INSERT INTO %s VALUES (?, ?)"
                                           table))]
    (doseq [[a b] pairs]
      (.setLong ps 1 a)
      (.setLong ps 2 b)
      (.addBatch ps))
    (.executeBatch ps)
    (.close ps))
  (execute! conn (format "CREATE INDEX idx_%s_b ON %s(b)" table table))
  (execute! conn (format "ANALYZE %s" table)))

(defn setup-join1-tables!
  [^Connection conn relations]
  (doseq [relation [:d1 :d2 :c2 :c3 :c4]]
    (setup-relation! conn (name relation) (get relations relation))))

;; =============================================================================
;; Benchmark Runners
;; =============================================================================

(defn run-portable-benchmark
  [{:keys [family binding bound-value query spec] :as task}]
  (let [task-data (core/generate-task-data task)]
    (with-open [conn (get-connection)]
      (case family
        :tc (setup-tc-table! conn task-data)
        :sg (setup-sg-table! conn task-data)
        :join1 (setup-join1-tables! conn task-data))
      (System/gc)
      (let [sql (case family
                  :tc (tc-query-for binding bound-value)
                  :sg (sg-query-for binding bound-value)
                  :join1 (join1-query-for query binding bound-value))
            [rows time-ms] (core/time-once (query-rows conn sql))]
        {:system "postgresql"
         :benchmark spec
         :time-ms time-ms
         :result-count (count rows)
         :base-fact-count (core/task-base-fact-count task task-data)
         :input-digest (core/task-data-digest task task-data)
         :engine-version (.getDatabaseProductVersion (.getMetaData conn))
         :timing-scope :query-and-materialization
         :status :ok}))))

(defn run-tc-benchmark
  "Run TC benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [edges (data/generate-tc-instance (keyword instance-name))]
    (try
      (let [conn (get-connection)]
        (try
          (setup-tc-table! conn edges)
          (System/gc)
          (let [[rows time-ms] (core/time-once (query-rows conn tc-query))]
            {:system "postgresql"
             :benchmark (str "tc:" instance-name)
             :time-ms time-ms
             :result-count (count rows)
             :status :ok})
          (finally
            (.close conn))))
      (catch Exception e
        (println "Error:" (.getMessage e))
        {:system "postgresql" :benchmark (str "tc:" instance-name) :status :error}))))

(defn run-sg-benchmark
  "Run SG benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [relations (data/generate-sg-instance (keyword instance-name))]
    (try
      (let [conn (get-connection)]
        (try
          (setup-sg-table! conn relations)
          (System/gc)
          (let [[rows time-ms] (core/time-once (query-rows conn sg-query))]
            {:system "postgresql"
             :benchmark (str "sg:" instance-name)
             :time-ms time-ms
             :result-count (count rows)
             :status :ok})
          (finally
            (.close conn))))
      (catch Exception e
        (println "Error:" (.getMessage e))
        {:system "postgresql" :benchmark (str "sg:" instance-name) :status :error}))))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(def default-benchmarks
  ["tc:50k-cyclic-ff" "sg:6k-cyclic-ff"])

(defn parse-benchmark [spec]
  (core/parse-benchmark spec))

(defn run-benchmark [spec]
  (try
    (run-portable-benchmark (core/require-benchmark-task spec))
    (catch Exception e
      (println "Error:" (.getMessage e))
      {:system "postgresql" :benchmark spec :status :error
       :error (.getMessage e)})))

(defn run-benchmarks [benchmark-specs]
  (doall (map run-benchmark benchmark-specs)))

(defn -main [& args]
  (let [report (try
                 (core/run-system-cli! "postgresql" default-benchmarks
                                       run-benchmark args)
                 (finally
                   (shutdown-agents)))]
    (System/exit (:exit-code report))))
