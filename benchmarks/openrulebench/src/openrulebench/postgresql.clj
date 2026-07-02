(ns openrulebench.postgresql
  "PostgreSQL benchmarks for OpenRuleBench using recursive CTEs via JDBC.

   Requires: PostgreSQL running locally.
   Connection: jdbc:postgresql://localhost:5432/postgres"
  (:require
   [openrulebench.core :as core]
   [openrulebench.data :as data]
   [clojure.string :as str])
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
     SELECT DISTINCT child, child FROM parent
     UNION
     SELECT DISTINCT p1.child, p2.child
     FROM parent p1, parent p2, sg
     WHERE p1.parent = sg.x AND p2.parent = sg.y
   )
   SELECT x, y FROM sg;")

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

(defn setup-tc-table!
  "Create and populate edge table for TC benchmark."
  [^Connection conn edges]
  (execute! conn "DROP TABLE IF EXISTS edge")
  (execute! conn "CREATE TABLE edge (a INTEGER, b INTEGER)")
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
  "Create and populate parent table for SG benchmark."
  [^Connection conn edges]
  ;; edges are [parent child]
  (execute! conn "DROP TABLE IF EXISTS parent")
  (execute! conn "CREATE TABLE parent (child INTEGER, parent INTEGER)")
  ;; Batch insert
  (let [ps (.prepareStatement conn "INSERT INTO parent VALUES (?, ?)")]
    (doseq [[p c] edges]
      (.setLong ps 1 c)  ; child
      (.setLong ps 2 p)  ; parent
      (.addBatch ps))
    (.executeBatch ps)
    (.close ps))
  ;; Create indexes and analyze
  (execute! conn "CREATE INDEX idx_parent_child ON parent(child)")
  (execute! conn "CREATE INDEX idx_parent_parent ON parent(parent)")
  (execute! conn "ANALYZE parent"))

;; =============================================================================
;; Benchmark Runners
;; =============================================================================

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
  (let [edges (data/generate-sg-instance (keyword instance-name))]
    (try
      (let [conn (get-connection)]
        (try
          (setup-sg-table! conn edges)
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
        {:system "postgresql" :benchmark spec :status :error})
      (catch Exception e
        (println "Error:" (.getMessage e))
        {:system "postgresql" :benchmark spec :status :error}))))

(defn run-benchmarks [benchmark-specs]
  (doall (map run-benchmark benchmark-specs)))

(defn -main [& args]
  (let [benchmarks (if (seq args) args default-benchmarks)
        results (run-benchmarks benchmarks)]
    (core/print-row "postgresql" results)))
