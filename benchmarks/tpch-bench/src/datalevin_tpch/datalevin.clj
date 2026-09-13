(ns datalevin-tpch.datalevin
  "Load TPC-H data into Datalevin and run the Datalog query set."
  (:require
   [clojure.java.io :as io]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin-bench.host :as host]
   [datalevin-tpch.common :as c]
   [datalevin-tpch.queries :as q]))

;; ---------------------------------------------------------------------------
;; Schema

(def schema
  (into {}
        (for [table c/table-order
              :let  [{:keys [types]} (c/table-specs table)]
              [a t] (map vector (c/attrs table) types)]
          [a {:db/valueType (c/type->db-type t)}])))

;; ---------------------------------------------------------------------------
;; Load

(defn- load-table!
  [db table]
  (let [{:keys [types]} (c/table-specs table)
        as   (c/attrs table)
        eid  (atom 0)
        t0   (System/nanoTime)]
    (c/tbl-rows
     table
     (fn [rows]
       (d/fill-db
        db
        (eduction
         (mapcat
          (fn [row]
            (let [e (swap! eid inc)]
              (mapv (fn [a t v] (d/datom e a (c/parse-value t v)))
                    as types row))))
         rows))))
    (println (format "  %-9s %10d rows  %6.2fs" table @eid
                     (/ (- (System/nanoTime) t0) 1.0e9)))))

(defn db
  "Load TPC-H .tbl files into a fresh Datalevin database.

  Options:
    :dir  database directory (default \"db\" under the project root)"
  [{:keys [dir] :or {dir "db"}}]
  (c/require-data!)
  (let [dir (io/file c/base-dir dir)]
    (when (.exists dir)
      (println "Removing existing database at" (.getPath dir))
      (u/delete-files dir))
    (println "Loading TPC-H data into Datalevin at" (.getPath dir))
    (let [t0 (System/nanoTime)
          db (d/empty-db (.getPath dir) schema {:closed-schema? true})]
      (try
        (doseq [table c/table-order]
          (load-table! db table))
        (println (format "Load time: %.2fs" (/ (- (System/nanoTime) t0) 1.0e9)))
        (println "Running analyze...")
        (let [a0 (System/nanoTime)]
          (d/analyze db)
          (println (format "Analyze time: %.2fs"
                           (/ (- (System/nanoTime) a0) 1.0e9))))
        (finally
          (d/close-db db)))))
  (println "Done. Datalevin database created.")
  (shutdown-agents)
  (System/exit 0))

;; ---------------------------------------------------------------------------
;; Benchmark

(defn- selected-ids
  [queries]
  (cond
    (or (nil? queries) (= queries :all)) (q/query-ids)
    (sequential? queries) (mapv long queries)
    :else (throw (ex-info "Unsupported :queries value" {:queries queries}))))

(defn- result-rows
  "Row count for either a collection result or a scalar aggregate result."
  [res]
  (cond
    (nil? res)   0
    (coll? res)  (count res)
    :else        1))

(defn- run-one
  "Run one Datalog query and return elapsed milliseconds. `explain?` also
  reports preparation time and the remaining wall time, including sorting."
  [db n explain?]
  (let [query (q/datalog n)
        t0    (System/nanoTime)]
    (if explain?
      (try
        (let [res        (d/explain {:run? true :intermediate-counts? false} query db)
              rows       (result-rows (:result res))
              wall-ms    (/ (- (System/nanoTime) t0) 1.0e6)
              prepare-ms (some-> (:prepare-time res) str parse-double)]
          {:query n
           :rows rows
           :prepare-ms (:prepare-time res)
           ;; Some execution paths record :execution-time before ORDER BY.
           ;; Measure through return so the CSV always includes the full sort.
           :exec-ms (when prepare-ms
                      (format "%.3f" (max 0.0 (- wall-ms prepare-ms))))
           :wall-ms wall-ms})
        (catch Throwable _
          (run-one db n false)))
      (let [res (d/q query db)]
        {:query n
         :rows  (result-rows res)
         :wall-ms (/ (- (System/nanoTime) t0) 1.0e6)}))))

(defn bench
  "Run the TPC-H Datalog query set against an existing Datalevin database.

  Options:
    :dir      database directory (default \"db\")
    :queries  vector of query numbers, or :all (default :all)
    :out      CSV output path (default \"datalevin_pass.csv\")
    :explain? report preparation and remaining wall time (default true)"
  [{:keys [dir queries out explain?]
    :or   {dir "db" out "datalevin_pass.csv" explain? true}}]
  (let [conn (d/get-conn (.getPath (io/file c/base-dir dir)))
        ids  (selected-ids queries)]
    (try
      (host/with-paused-media
        (with-open [w (io/writer (io/file c/base-dir out))]
          (d/write-csv w [["Query" "Rows" "Prepare (ms)" "Execution (ms)"
                           "Wall (ms)"]])
          (doseq [n ids]
            (print (format "  q%-2d ... " n)) (flush)
            (let [{:keys [rows prepare-ms exec-ms wall-ms]} (run-one (d/db conn) n explain?)]
              (println (format "%,.1f ms" (double wall-ms)))
              (d/write-csv w [[n rows
                               (or prepare-ms "")
                               (or exec-ms "")
                               (format "%.3f" (double wall-ms))]])))))
      (finally
        (d/close conn))))
  (println "Results written to" out)
  (shutdown-agents)
  (System/exit 0))

(defn -main
  [& args]
  (bench (if (seq args) {:out (first args)} {})))
