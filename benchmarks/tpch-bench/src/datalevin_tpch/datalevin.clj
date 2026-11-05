(ns datalevin-tpch.datalevin
  "Load TPC-H data into Datalevin and run the Datalog query set."
  (:require
   [clojure.java.io :as io]
   [datalevin.core :as d]
   [datalevin.query :as query]
   [datalevin.query.cache :as cache]
   [datalevin.query.plan :as plan]
   [datalevin.util :as u]
   [datalevin-bench.host :as host]
   [datalevin-tpch.common :as c]
   [datalevin-tpch.queries :as q]))

;; ---------------------------------------------------------------------------
;; Schema

(def ^:private primary-keys
  ;; The components of partsupp and lineitem's composite keys are not unique.
  #{:region/regionkey :nation/nationkey :part/partkey :supplier/suppkey
    :customer/custkey :orders/orderkey})

(def ^:private composite-keys
  {"partsupp" [:partsupp/part+supplier [:partsupp/part :partsupp/supplier]]
   "lineitem" [:lineitem/order+linenumber [:lineitem/order :lineitem/linenumber]]})

(def references
  "SQL foreign-key columns mapped to [reference attribute target table]."
  {:nation/regionkey   [:nation/region "region"]
   :supplier/nationkey [:supplier/nation "nation"]
   :customer/nationkey [:customer/nation "nation"]
   :partsupp/partkey   [:partsupp/part "part"]
   :partsupp/suppkey   [:partsupp/supplier "supplier"]
   :orders/custkey     [:orders/customer "customer"]
   :lineitem/orderkey  [:lineitem/order "orders"]
   :lineitem/partkey   [:lineitem/part "part"]
   :lineitem/suppkey   [:lineitem/supplier "supplier"]})

(defn attribute
  "Datalevin attribute corresponding to a SQL column attribute."
  [a]
  (if-let [[ref] (references a)] ref a))

(def schema
  (into (into {}
              (for [[_ [attr components]] composite-keys]
                [attr {:db/valueType :db.type/tuple
                       :db/tupleAttrs components
                       :db/unique :db.unique/value}]))
        (for [table c/table-order
              :let  [{:keys [types]} (c/table-specs table)]
              [a t] (map vector (c/attrs table) types)]
          [(attribute a)
           (cond-> {:db/valueType (if (references a)
                                   :db.type/ref
                                   (c/type->db-type t))}
             (primary-keys a) (assoc :db/unique :db.unique/value))])))

;; ---------------------------------------------------------------------------
;; Load

(defn entity-layout
  "Allocate disjoint ranges as in JOB. Single-column keys use base + key;
  composite-key rows use base + row ordinal, since no table references them.
  Scan actual maxima (orders have sparse keys), not row counts or assumed SF.
  `read-rows` has the same interface as common/tbl-rows."
  [read-rows]
  (first
    (reduce
      (fn [[layout base] table]
        (let [composite? (contains? composite-keys table)
              [row-count maximum]
              (read-rows table
                (fn [rows]
                  (reduce (fn [[n maximum] row]
                            (if composite?
                              [(inc n) (inc n)]
                              (let [key (Long/parseLong (first row))]
                                (when (neg? key)
                                  (throw (ex-info "Negative TPC-H key"
                                                  {:table table :key key})))
                                [(inc n) (max maximum key)])))
                          [0 0] rows)))]
          [(assoc layout table {:base base :maximum maximum :rows row-count})
           (+ base maximum 1)]))
      [{} 1] c/table-order)))

(defn entity-id
  "Convert a source key (or composite-row ordinal) to its positive entity ID."
  [layout table key]
  (let [{:keys [base maximum]} (get layout table)]
    (when-not (and base (integer? key) (<= 0 key maximum))
      (throw (ex-info "TPC-H key outside allocated entity range"
                      {:table table :key key :range (get layout table)})))
    (+ base key)))

(defn- table-row-datoms
  "Construct entity IDs and refs directly before passing trusted datoms to fill-db."
  [layout table]
  (let [columns                 (c/attrs table)
        attrs                   (mapv attribute columns)
        types                   (:types (c/table-specs table))
        [tuple-attr components] (composite-keys table)
        positions               (mapv #(.indexOf ^java.util.List attrs %)
                                      components)]
    (fn [ordinal row]
      (let [parsed (mapv c/parse-value types row)
            e (entity-id layout table (if tuple-attr ordinal (first parsed)))
            values (mapv (fn [a value]
                           (if-let [[_ target] (references a)]
                             (entity-id layout target value)
                             value)) columns parsed)
            datoms (mapv #(d/datom e %1 %2) attrs values)]
        (cond-> datoms
          tuple-attr (conj (d/datom e tuple-attr (mapv values positions))))))))

(defn- load-table!
  [db layout read-rows table]
  (let [row-datoms (table-row-datoms layout table)
        t0         (System/nanoTime)]
    (read-rows
     table
     (fn [rows]
       (d/fill-db
        db
        (eduction
         ;; map-indexed starts afresh if a map resize retries the bulk load.
         ;; An external counter would assign different IDs on the retry.
         (comp (map-indexed (fn [i row] (row-datoms (inc i) row))) cat)
         rows))))
    (println (format "  %-9s %10d rows  %6.2fs" table (get-in layout [table :rows])
                     (/ (- (System/nanoTime) t0) 1.0e9)))))

(defn load-data!
  "Load every table using one shared entity layout. Returns the layout."
  ([db] (load-data! db c/tbl-rows))
  ([db read-rows]
   (let [layout (entity-layout read-rows)]
     (doseq [table c/table-order]
       (load-table! db layout read-rows table))
     layout)))

(defn require-reference-schema!
  "Reject databases built with the former scalar-key model; they need a rebuild."
  [conn]
  (let [actual (d/schema conn)]
    (doseq [[_ [a _]] references]
      (when-not (= :db.type/ref (:db/valueType (actual a)))
        (throw (ex-info "TPC-H reference schema missing; rebuild with :datalevin-db"
                        {:attribute a}))))))

(defn db
  "Load TPC-H .tbl files into a fresh Datalevin database.

  Options:
    :dir      database directory (default \"db\" under the project root)
    :mapsize  initial map size in MiB (default 100000, as in JOB)"
  [{:keys [dir mapsize] :or {dir "db" mapsize 100000}}]
  (when-not (pos-int? mapsize)
    (throw (ex-info ":mapsize must be a positive number of MiB" {:mapsize mapsize})))
  (c/require-data!)
  (let [dir (io/file c/base-dir dir)]
    (when (.exists dir)
      (println "Removing existing database at" (.getPath dir))
      (u/delete-files dir))
    (println "Loading TPC-H data into Datalevin at" (.getPath dir))
    (let [t0 (System/nanoTime)
          db (d/empty-db (.getPath dir) schema
                         {:closed-schema? true :kv-opts {:mapsize mapsize}})]
      (try
        (load-data! db)
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
  "Row count for a collection or a scalar aggregate, including SQL NULL."
  [res]
  (if (coll? res) (count res) 1))

(defn- run-one
  "Run one Datalog query and return elapsed milliseconds. `explain?` also
  reports preparation time and the remaining wall time, including sorting.
  Result caching is disabled in both modes, including nested queries."
  [db n explain?]
  (binding [query/*cache?* false cache/*cache?* false]
    (let [query (q/datalog n)
          t0    (System/nanoTime)]
      (if explain?
        (let [explanation (volatile! {})
              ;; Explain's :result can precede ORDER BY/LIMIT. Retain the
              ;; actual return value while collecting the same phase timings.
              result     (binding [plan/*explain* explanation
                                   plan/*intermediate-counts?* false
                                   plan/*start-time* t0]
                           (d/q query db))
              res        @explanation
              rows       (result-rows result)
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
        (let [res (d/q query db)]
          {:query n
           :rows  (result-rows res)
           :wall-ms (/ (- (System/nanoTime) t0) 1.0e6)})))))

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
      (require-reference-schema! conn)
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
