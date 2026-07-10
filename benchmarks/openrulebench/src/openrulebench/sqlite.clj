(ns openrulebench.sqlite
  "SQLite benchmarks for OpenRuleBench using recursive CTEs via JDBC."
  (:require
   [openrulebench.core :as core]
   [openrulebench.data :as data]
   [clojure.java.io :as io]
   [clojure.string :as str])
  (:import
   [java.sql DriverManager Connection Statement ResultSet]
   [java.util UUID]))

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

;; =============================================================================
;; Database Setup
;; =============================================================================

(defn create-connection
  "Create a SQLite connection."
  [db-path]
  (Class/forName "org.sqlite.JDBC")
  (DriverManager/getConnection (str "jdbc:sqlite:" db-path)))

(defn execute!
  "Execute a SQL statement."
  [^Connection conn sql]
  (let [stmt (.createStatement conn)]
    (.executeUpdate stmt sql)
    (.close stmt)))

(defn query-rows
  "Run a query and return all rows as a vector of pairs."
  [^Connection conn sql]
  (let [stmt (.createStatement conn)
        rs (.executeQuery stmt sql)]
    (loop [acc (transient [])]
      (if (.next rs)
        (recur (conj! acc [(.getLong rs 1) (.getLong rs 2)]))
        (let [rows (persistent! acc)]
          (.close rs)
          (.close stmt)
          rows)))))

(defn load-tc-data!
  "Load edge data into SQLite for TC benchmark."
  [^Connection conn edges]
  (execute! conn "CREATE TABLE edge (a INTEGER, b INTEGER)")
  (.setAutoCommit conn false)
  (let [ps (.prepareStatement conn "INSERT INTO edge VALUES (?, ?)")]
    (doseq [[a b] edges]
      (.setLong ps 1 a)
      (.setLong ps 2 b)
      (.addBatch ps))
    (.executeBatch ps)
    (.close ps))
  (.commit conn)
  (.setAutoCommit conn true)
  ;; Create index and analyze
  (execute! conn "CREATE INDEX idx_edge_a ON edge(a)")
  (execute! conn "CREATE INDEX idx_edge_b ON edge(b)")
  (execute! conn "ANALYZE"))

(defn load-sg-data!
  "Load par/sib data into SQLite for SG benchmark."
  [^Connection conn {:keys [par sib]}]
  (execute! conn "CREATE TABLE par (a INTEGER, b INTEGER)")
  (execute! conn "CREATE TABLE sib (a INTEGER, b INTEGER)")
  (.setAutoCommit conn false)
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
  (.commit conn)
  (.setAutoCommit conn true)
  ;; Create index and analyze
  (execute! conn "CREATE INDEX idx_par_a ON par(a)")
  (execute! conn "CREATE INDEX idx_par_b ON par(b)")
  (execute! conn "CREATE INDEX idx_sib_a ON sib(a)")
  (execute! conn "CREATE INDEX idx_sib_b ON sib(b)")
  (execute! conn "ANALYZE"))

;; =============================================================================
;; Benchmark Runners
;; =============================================================================

(defn run-tc-benchmark
  "Run TC benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [edges (data/generate-tc-instance (keyword instance-name))
        db-path (str "/tmp/openrulebench-tc-" (UUID/randomUUID) ".db")]
    (try
      (let [conn (create-connection db-path)]
        (try
          (load-tc-data! conn edges)
          (System/gc)
          (let [[rows time-ms] (core/time-once (query-rows conn tc-query))]
            {:system "sqlite"
             :benchmark (str "tc:" instance-name)
             :time-ms time-ms
             :result-count (count rows)
             :status :ok})
          (finally
            (.close conn))))
      (catch Exception e
        (println "Error:" (.getMessage e))
        {:system "sqlite" :benchmark (str "tc:" instance-name) :status :error})
      (finally
        (io/delete-file db-path true)))))

(defn run-sg-benchmark
  "Run SG benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [relations (data/generate-sg-instance (keyword instance-name))
        db-path (str "/tmp/openrulebench-sg-" (UUID/randomUUID) ".db")]
    (try
      (let [conn (create-connection db-path)]
        (try
          (load-sg-data! conn relations)
          (System/gc)
          (let [[rows time-ms] (core/time-once (query-rows conn sg-query))]
            {:system "sqlite"
             :benchmark (str "sg:" instance-name)
             :time-ms time-ms
             :result-count (count rows)
             :status :ok})
          (finally
            (.close conn))))
      (catch Exception e
        (println "Error:" (.getMessage e))
        {:system "sqlite" :benchmark (str "sg:" instance-name) :status :error})
      (finally
        (io/delete-file db-path true)))))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(def default-benchmarks
  ["tc:small" "tc:medium" "sg:small"])

(defn parse-benchmark [spec]
  (let [[bench-type instance] (str/split spec #":")]
    [bench-type instance]))

(defn run-benchmark [spec]
  (let [[bench-type instance] (parse-benchmark spec)]
    (try
      (case bench-type
        "tc" (run-tc-benchmark instance)
        "sg" (run-sg-benchmark instance)
        {:system "sqlite" :benchmark spec :status :error})
      (catch Exception e
        (println "Error:" (.getMessage e))
        {:system "sqlite" :benchmark spec :status :error}))))

(defn run-benchmarks [benchmark-specs]
  (doall (map run-benchmark benchmark-specs)))

(defn -main [& args]
  (let [benchmarks (if (seq args) args default-benchmarks)
        results (run-benchmarks benchmarks)]
    (core/print-row "sqlite" results)))
