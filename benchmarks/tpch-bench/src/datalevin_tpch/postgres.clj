(ns datalevin-tpch.postgres
  "Load TPC-H data into PostgreSQL and run the query set.

  PostgreSQL is optional: it requires a running server reachable through a JDBC
  URL. Connection settings come from JVM properties (`pg.url`, `pg.user`,
  `pg.pass`) or the options map."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin.core :as d]
   [datalevin-tpch.common :as c]
   [datalevin-bench.host :as host])
  (:import
   [java.io File PipedReader PipedWriter]
   [java.sql Connection DriverManager]
   [java.util.concurrent Executors TimeUnit TimeoutException]
   [org.postgresql PGConnection]
   [org.postgresql.copy CopyManager]))

(def default-url
  (System/getProperty "pg.url" "jdbc:postgresql://localhost:5432/postgres"))

(defn- conn-opts
  [{:keys [url user pass] :or {url default-url}}]
  [url
   (or user (System/getProperty "pg.user" (System/getenv "USER")))
   (or pass (System/getProperty "pg.pass" ""))])

(defn- get-connection ^Connection [opts]
  (DriverManager/getConnection (first opts) (second opts) (nth opts 2)))

(defn- strip-sql-comments [s] (s/replace s #"(?m)--[^\n]*" ""))

(defn- exec-sql-file! [^Connection conn ^File f]
  (with-open [stmt (.createStatement conn)]
    (doseq [ddl (->> (s/split (strip-sql-comments (slurp f)) #";\s*")
                     (map s/trim)
                     (remove s/blank?))]
      (.executeUpdate stmt ddl))))

(defn- strip-trailing-pipe [^String line]
  (if (and (pos? (.length line)) (= \| (.charAt line (dec (.length line)))))
    (subs line 0 (dec (.length line)))
    line))

(defn- copy-tbl!
  "COPY a pipe-delimited .tbl file after removing the trailing delimiter that
  dbgen appends to every line. A pipe reader streams the rewrite so no
  intermediate file is created."
  [^Connection conn table]
  (let [mgr (CopyManager. (.unwrap conn PGConnection))
        ;; QUOTE is set to a byte that cannot appear in the data so CSV parsing
        ;; never treats a value as quoted.
        sql (str "COPY " table " FROM STDIN WITH (FORMAT csv, DELIMITER '|',"
                 " QUOTE E'\\x01', NULL '')")
        pr  (PipedReader.)
        pw  (PipedWriter. pr)
        producer (future
                   (try
                     (with-open [rdr (io/reader (c/tbl-file table))]
                       (doseq [line (line-seq rdr)]
                         (.write pw (str (strip-trailing-pipe line) "\n"))))
                     (finally
                       (.close pw))))]
    (.copyIn mgr sql pr)
    @producer))

(defn db
  "Load TPC-H .tbl files into PostgreSQL.

  Options:
    :url  JDBC URL (default jdbc:postgresql://localhost:5432/postgres)
    :user PostgreSQL user (default $USER)
    :pass PostgreSQL password"
  [opts]
  (c/require-data!)
  (let [c-opts (conn-opts opts)]
    (with-open [conn (get-connection c-opts)]
      (with-open [stmt (.createStatement conn)]
        (doseq [t (reverse c/table-order)]
          (try (.executeUpdate stmt (str "DROP TABLE IF EXISTS " t " CASCADE"))
               (catch Exception _))))
      (println "Creating schema...")
      (exec-sql-file! conn (io/file (c/data-dir) "schema-postgres.sql"))
      (let [t0 (System/nanoTime)]
        (doseq [table c/table-order]
          (println "  Loading" table "...")
          (copy-tbl! conn table))
        (println (format "Load time: %.2fs"
                         (/ (- (System/nanoTime) t0) 1.0e9))))
      (println "Creating indexes...")
      (exec-sql-file! conn (io/file (c/data-dir) "indexes-postgres.sql"))
      (println "Running ANALYZE...")
      (with-open [stmt (.createStatement conn)]
        (.executeUpdate stmt "ANALYZE")))
    (println "Done. Data loaded into PostgreSQL."))
  (shutdown-agents)
  (System/exit 0))

;; ---------------------------------------------------------------------------
;; Benchmark

(defn- query-numbers
  [queries]
  (cond
    (or (nil? queries) (= queries :all)) (range 1 23)
    (sequential? queries) (map long queries)
    :else (throw (ex-info "Unsupported :queries value" {:queries queries}))))

(defn bench
  "Run the TPC-H PostgreSQL query set using EXPLAIN (ANALYZE, FORMAT JSON).

  Options:
    :url      JDBC URL
    :user     PostgreSQL user
    :pass     PostgreSQL password
    :queries  vector of query numbers, or :all
    :out      CSV output path (default \"postgres_pass.csv\")"
  [{:keys [out queries] :or {out "postgres_pass.csv"} :as opts}]
  (let [c-opts (conn-opts opts)]
    (host/with-paused-media
      (with-open [conn (get-connection c-opts)
                  w    (io/writer (io/file c/base-dir out))]
      (d/write-csv w [["Query" "Planning (ms)" "Execution (ms)"]])
      (doseq [n (query-numbers queries)]
        (let [sql (str "EXPLAIN (ANALYZE, FORMAT JSON) "
                       (s/replace (s/trim (slurp (c/query-file :postgres n)))
                                  #";\s*$" ""))
              stmt (.createStatement conn)]
          (print (format "  q%-2d ... " n)) (flush)
          (try
            (let [rs (.executeQuery stmt sql)]
              (when (.next rs)
                (let [json     (.getString rs 1)
                      plan-time (second (re-find #"\"Planning Time\": ([0-9.]+)" json))
                      exec-time (second (re-find #"\"Execution Time\": ([0-9.]+)" json))]
                  (println (str plan-time " + " exec-time " ms"))
                  (d/write-csv w [[n plan-time exec-time]])))
              (.close rs))
            (catch Exception e
              (println "ERROR:" (.getMessage e))
              (d/write-csv w [[n "error" ""]]))
            (finally
              (.close stmt))))))))
  (println "Results written to" out)
  (shutdown-agents)
  (System/exit 0))

(defn -main
  [& args]
  (bench (if (seq args) {:out (first args)} {})))
