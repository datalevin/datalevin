(ns datalevin-bench.postgres
  (:require
   [datalevin.core :as d]
   [clojure.java.io :as io]
   [clojure.string :as s])
  (:import
   [java.sql DriverManager Connection]
   [org.postgresql.copy CopyManager]
   [org.postgresql PGConnection]))

(def db-url "jdbc:postgresql://localhost:5432/postgres")
(def db-user (System/getProperty "pg.user" (System/getenv "USER")))
(def db-pass (System/getProperty "pg.pass" ""))

(def tables
  ["aka_name" "aka_title" "cast_info" "char_name" "comp_cast_type"
   "company_name" "company_type" "complete_cast" "info_type" "keyword"
   "kind_type" "link_type" "movie_companies" "movie_info" "movie_info_idx"
   "movie_keyword" "movie_link" "name" "person_info" "role_type" "title"])

(defn- get-connection ^Connection []
  (DriverManager/getConnection db-url db-user db-pass))

(defn- load-csv!
  "Load a CSV file into a PostgreSQL table using COPY."
  [^Connection conn table-name]
  (let [csv-file (io/file (str "data/" table-name ".csv"))
        copy-mgr (CopyManager. (.unwrap conn PGConnection))]
    (when (.exists csv-file)
      (println "  Loading" table-name "...")
      (with-open [rdr (io/reader csv-file)]
        (.copyIn copy-mgr
                 (str "COPY " table-name " FROM STDIN WITH (FORMAT csv, ESCAPE E'\\\\')")
                 rdr)))))

(defn db
  "Load CSV data into PostgreSQL."
  [& _opts]
  (println "Loading data into PostgreSQL, please wait...")
  (with-open [conn (get-connection)]
    ;; Drop and recreate tables
    (let [stmt (.createStatement conn)]
      (println "Dropping existing tables...")
      (doseq [t (reverse tables)]
        (try (.executeUpdate stmt (str "DROP TABLE IF EXISTS " t " CASCADE"))
             (catch Exception _)))
      (.close stmt))
    ;; Create tables
    (let [stmt (.createStatement conn)]
      (println "Creating schema...")
      (doseq [ddl (s/split (slurp "data/schema.sql") #";\s*")]
        (let [ddl (s/trim ddl)]
          (when (seq ddl)
            (.executeUpdate stmt ddl))))
      (.close stmt))
    ;; Load CSVs
    (let [start (System/nanoTime)]
      (doseq [table tables]
        (load-csv! conn table))
      (println (format "Load time: %.3f s" (/ (- (System/nanoTime) start) 1e9))))
    ;; Create indexes
    (println "Creating indexes...")
    (let [start (System/nanoTime)
          stmt  (.createStatement conn)]
      (doseq [line (s/split-lines (slurp "data/fkindexes.sql"))]
        (let [line (s/trim line)]
          (when (seq line)
            (.executeUpdate stmt line))))
      (.close stmt)
      (println (format "Index time: %.3f s" (/ (- (System/nanoTime) start) 1e9))))
    ;; Analyze
    (println "Running ANALYZE...")
    (let [start (System/nanoTime)
          stmt  (.createStatement conn)]
      (.executeUpdate stmt "ANALYZE")
      (.close stmt)
      (println (format "Analyze time: %.3f s" (/ (- (System/nanoTime) start) 1e9)))))
  (println "Done. Data loaded into PostgreSQL."))

(defn- query-files
  "Return sorted list of .sql files in the queries directory."
  []
  (->> (.listFiles (io/file "queries"))
       (filter #(and (.isFile %) (s/ends-with? (.getName %) ".sql")))
       (sort-by #(.getName %))))

(defn -main
  "Run JOB benchmark queries against PostgreSQL using EXPLAIN ANALYZE."
  [& _opts]
  (println "Running JOB benchmark against PostgreSQL...")
  (let [result-file "postgres_onepass_time.csv"]
    (with-open [conn (get-connection)
                w    (io/writer result-file)]
      (d/write-csv w [["Query Name" "Planning Time (ms)" "Execution Time (ms)"]])
      (doseq [f (query-files)]
        (let [qname (s/replace (.getName f) ".sql" "")
              sql   (str "EXPLAIN (ANALYZE, FORMAT JSON) "
                         (s/replace (s/trim (slurp f)) #";\s*$" ""))
              _     (print (str "  " qname "... "))
              stmt  (.createStatement conn)
              rs    (.executeQuery stmt sql)]
          (when (.next rs)
            (let [json-str  (.getString rs 1)
                  plan-time (second (re-find #"\"Planning Time\": ([0-9.]+)" json-str))
                  exec-time (second (re-find #"\"Execution Time\": ([0-9.]+)" json-str))]
              (println (str plan-time " + " exec-time " ms"))
              (d/write-csv w [[qname plan-time exec-time]])))
          (.close rs)
          (.close stmt))))
    (println "Done. Results written to" result-file)))
