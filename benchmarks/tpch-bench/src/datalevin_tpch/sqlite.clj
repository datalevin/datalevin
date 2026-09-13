(ns datalevin-tpch.sqlite
  "Load TPC-H data into SQLite and run the SQLite-dialect query set.

  SQLite is the local reference implementation: its results are the oracle the
  Datalevin translations are checked against. It uses WAL and a memory map for
  read performance, matching the JOB-bench SQLite runner."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin.core :as d]
   [datalevin-tpch.common :as c])
  (:import
   [java.io File]
   [java.sql Connection DriverManager]
   [java.util.concurrent Executors TimeUnit TimeoutException]))

(def default-db-name "sqlite.db")

(defn db-path ^File ([] (db-path default-db-name)) ([p] (io/file c/base-dir p)))

(defn- db-url [^File f] (str "jdbc:sqlite:" (.getPath f)))

(defn- strip-sql-comments
  [s]
  (s/replace s #"(?m)--[^\n]*" ""))

(defn- exec-sql-file!
  [^Connection conn ^File f]
  (with-open [stmt (.createStatement conn)]
    (doseq [ddl (->> (s/split (strip-sql-comments (slurp f)) #";\s*")
                     (map s/trim)
                     (remove s/blank?))]
      (.executeUpdate stmt ddl))))

(defn- set-param!
  [^java.sql.PreparedStatement ps idx t ^String v]
  (case t
    :long   (.setLong ps idx (Long/parseLong v))
    :double (.setDouble ps idx (Double/parseDouble v))
    (.setString ps idx v)))

(defn- load-table!
  [^Connection conn table]
  (let [{:keys [columns types]} (c/table-specs table)
        n            (count columns)
        placeholders (s/join "," (repeat n "?"))
        sql          (str "INSERT INTO " table " VALUES (" placeholders ")")
        t0           (System/nanoTime)
        cnt          (atom 0)]
    (c/tbl-rows
     table
     (fn [rows]
       (with-open [ps (.prepareStatement conn sql)]
         (doseq [row rows]
           (dotimes [i n]
             (set-param! ps (inc i) (nth types i) (nth row i)))
           (.addBatch ps)
           (when (zero? (mod (swap! cnt inc) 10000))
             (.executeBatch ps)))
         (.executeBatch ps))))
    (println (format "  %-9s %10d rows  %6.2fs" table @cnt
                     (/ (- (System/nanoTime) t0) 1.0e9)))))

(defn db
  "Load TPC-H .tbl files into a fresh SQLite database.

  Options:
    :path  database file (default \"sqlite.db\" under the project root)"
  [{:keys [path] :or {path default-db-name}}]
  (c/require-data!)
  (let [f (db-path path)]
    (when (.exists f)
      (println "Deleting existing" (.getPath f))
      (.delete f))
    (with-open [conn (DriverManager/getConnection (db-url f))]
      (println "Creating schema...")
      (exec-sql-file! conn (io/file (c/data-dir) "schema-sqlite.sql"))
      (.setAutoCommit conn false)
      (let [t0 (System/nanoTime)]
        (doseq [table c/table-order]
          (load-table! conn table))
        (.commit conn)
        (println (format "Load time: %.2fs"
                         (/ (- (System/nanoTime) t0) 1.0e9))))
      (.setAutoCommit conn true)
      (println "Creating indexes...")
      (exec-sql-file! conn (io/file (c/data-dir) "indexes-sqlite.sql"))
      (println "Running ANALYZE...")
      (with-open [stmt (.createStatement conn)]
        (.executeUpdate stmt "ANALYZE"))))
  (println "Done. SQLite database created at" (.getPath (db-path path)))
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
  "Run the TPC-H SQLite query set and write a CSV timing report.

  Options:
    :path     database file (default \"sqlite.db\")
    :queries  vector of query numbers, or :all
    :out      CSV output path (default \"sqlite_pass.csv\")
    :timeout  per-query timeout in seconds (default 120)"
  [{:keys [path queries out timeout]
    :or   {path default-db-name out "sqlite_pass.csv" timeout 120}}]
  (let [f (db-path path)]
    (with-open [conn (DriverManager/getConnection (db-url f))
                w    (io/writer (io/file c/base-dir out))]
      (with-open [stmt (.createStatement conn)]
        (.execute stmt "PRAGMA journal_mode=WAL")
        (.execute stmt "PRAGMA mmap_size=1073741824")
        ;; SQLite's LIKE is case-insensitive for ASCII by default; TPC-H and
        ;; PostgreSQL require case-sensitive matching, and Datalevin's `like`
        ;; is case-sensitive.
        (.execute stmt "PRAGMA case_sensitive_like=ON"))
      (let [pool (Executors/newSingleThreadExecutor)]
        (try
          (d/write-csv w [["Query" "Rows" "Wall (ms)"]])
          (doseq [n (query-numbers queries)]
            (let [qfile (c/query-file :sqlite n)
                  sql   (slurp qfile)
                  stmt  (.createStatement conn)
                  fut   (.submit pool
                                 ^Callable
                                 (fn []
                                   (let [t0 (System/nanoTime)
                                         rs (.executeQuery stmt sql)
                                         rows (loop [k 0] (if (.next rs) (recur (inc k)) k))]
                                     (.close rs)
                                     [rows (/ (- (System/nanoTime) t0) 1.0e6)])))]
              (print (format "  q%-2d ... " n)) (flush)
              (try
                (let [[rows ms] (.get fut timeout TimeUnit/SECONDS)]
                  (println (format "%,.1f ms (%d rows)" (double ms) rows))
                  (d/write-csv w [[n rows (format "%.3f" (double ms))]]))
                (catch TimeoutException _
                  (.cancel fut true)
                  (try (.cancel stmt) (catch Exception _))
                  (println "TIMEOUT")
                  (d/write-csv w [[n "timeout" ""]]))
                (catch Exception e
                  (println "ERROR:" (.getMessage e))
                  (d/write-csv w [[n "error" ""]]))
                (finally
                  (.close stmt)))))
          (finally
            (.shutdown pool))))))
  (println "Results written to" out)
  (shutdown-agents)
  (System/exit 0))

(defn -main
  [& args]
  (bench (if (seq args) {:out (first args)} {})))
