(ns ldbc-snb-bench.core
  "LDBC SNB Benchmark for Datalevin.

   Main entry point for loading data and running benchmarks.

   Usage:
     clj -M:load <data-dir>              ; Load LDBC SNB data
     clj -M:bench                        ; Run benchmark queries
     clj -M:bench IS1 IS2                ; Run specific queries
     clj -M:bench -o results.csv IS1     ; Run with custom results file
     clj -M:bench -p perf.csv            ; Run with custom perf file"
  (:require [clojure.java.io :as io]
            [clojure.stacktrace :as stacktrace]
            [clojure.string :as str]
            [datalevin.core :as d]
            [datalevin.query :as query]
            [ldbc-snb-bench.harness :as harness]
            [ldbc-snb-bench.schema :as schema]
            [ldbc-snb-bench.loader :as loader]
            [ldbc-snb-bench.parameters :as parameters]
            [ldbc-snb-bench.queries.interactive :as ic]
            [ldbc-snb-bench.queries.short :as is])
  (:import [java.time Instant ZoneOffset ZonedDateTime]
           [java.time.format DateTimeFormatter]
           [java.util Date]
           [datalevin.utl LRUCache]))

;; ============================================================
;; Configuration
;; ============================================================

(def db-path "db/ldbc-snb")
(def default-results-path "results/results.csv")
(def default-perf-path "results/perf.csv")
(def default-report-path "results/report.edn")
(def default-data-path "data")
(def debug-errors? (boolean (System/getenv "LDBC_BENCH_DEBUG")))
(def show-results? (boolean (System/getenv "LDBC_BENCH_SHOW_RESULTS")))

(def default-bench-options
  {:db-path db-path
   :results-path default-results-path
   :perf-path default-perf-path
   :output default-report-path
   :warmup 0
   :iterations 1
   :parameter-count 10
   :seed 42
   :scale-factor "1"
   :verify? true
   :query-cache? false})

(def ^DateTimeFormatter date-formatter
  (DateTimeFormatter/ofPattern "yyyy-MM-dd"))

(def ^DateTimeFormatter datetime-formatter
  (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

(defn- midnight-utc?
  [^Date d]
  (let [zdt (ZonedDateTime/ofInstant (.toInstant d) ZoneOffset/UTC)]
    (and (zero? (.getHour zdt))
         (zero? (.getMinute zdt))
         (zero? (.getSecond zdt))
         (zero? (.getNano zdt)))))

(defn- format-date
  [^Date d]
  (let [zdt (ZonedDateTime/ofInstant (.toInstant d) ZoneOffset/UTC)]
    (if (midnight-utc? d)
      (.format date-formatter zdt)
      (.format datetime-formatter zdt))))

(defn- escape-string
  [^String s]
  (str/replace s "\"" "\\\""))

(defn- format-value
  [v]
  (cond
    (nil? v) "null"
    (string? v) (str "\"" (escape-string v) "\"")
    (instance? Date v) (format-date v)
    (instance? Instant v) (.format datetime-formatter (ZonedDateTime/ofInstant v ZoneOffset/UTC))
    (.isArray (class v)) (str "[" (str/join ", " (map format-value (seq v))) "]")
    (sequential? v) (str "[" (str/join ", " (map format-value v)) "]")
    (set? v) (str "[" (str/join ", " (map format-value (sort-by str v))) "]")
    (keyword? v) (name v)
    :else (str v)))

(defn- row->cells
  [row]
  (cond
    (nil? row) []
    (.isArray (class row)) (vec row)
    (sequential? row) row
    :else [row]))

(defn- format-row
  [query row]
  (let [cells (row->cells row)
        values (cons query cells)]
    (str/join ", " (map format-value values))))

(defn- query-find-columns
  [query]
  (when (sequential? query)
    (let [after-find (next (drop-while #(not= % :find) query))]
      (when (seq after-find)
        (->> after-find
             (take-while #(not (keyword? %)))
             (map (fn [x]
                    (if (symbol? x)
                      (subs (str x) 1)
                      (str x))))
             (vec))))))

(defn- result-columns
  [result]
  (or (:columns result)
      (when-let [row (first (:rows result))]
        (mapv #(str "col" %) (range (count (row->cells row)))))))

;; ============================================================
;; Sample query parameters for benchmarking
;; These would typically come from LDBC substitution_parameters/
;; ============================================================

(defn to-date
  "Convert Instant to Date for Datalevin"
  [s]
  (parameters/to-date s))

(def sample-params
  "Sample parameters for benchmark queries.
   In a real benchmark, these come from LDBC's substitution_parameters files.
   Note: These IDs are from the SF1 LDBC SNB Datagen output."
  {:ic1  {:person-id  100
          :first-name "John"}
   :ic2  {:person-id 100
          :max-date  (to-date "2012-07-01T00:00:00Z")}
   :ic3  {:person-id      100
          :country-x-name "Germany"
          :country-y-name "France"
          :start-date     (to-date "2011-01-01T00:00:00Z")
          :duration-days  365}
   :ic4  {:person-id     100
          :start-date    (to-date "2011-01-01T00:00:00Z")
          :duration-days 365}
   :ic5  {:person-id 100
          :min-date  (to-date "2011-01-01T00:00:00Z")}
   :ic6  {:person-id 100
          :tag-name  "Mozart"}
   :ic7  {:person-id 100}
   :ic8  {:person-id 100}
   :ic9  {:person-id 100
          :max-date  (to-date "2012-07-01T00:00:00Z")}
   :ic10 {:person-id 100
          :month     5}
   :ic11 {:person-id      100
          :country-name   "Germany"
          :work-from-year 2010}
   :ic12 {:person-id      100
          :tag-class-name "MusicalArtist"}
   :ic13 {:person1-id 100
          :person2-id 6597069770569}
   :ic14 {:person1-id 100
          :person2-id 6597069770569}
   :is1  {:person-id 100}
   :is2  {:person-id 100}
   :is3  {:person-id 100}
   :is4  {:message-id 1099512606636}
   :is5  {:message-id 1099512606636}
   :is6  {:message-id 1099512606636}
   :is7  {:message-id 1099512606636}})

(def sample-result-counts
  "Known result counts for the bundled parameters against the validated SF1
   fixture used by this project. These are a fast pre-timing oracle; the test
   suite additionally checks representative values from every query."
  {:ic1 20, :ic2 20, :ic3 3, :ic4 10, :ic5 20, :ic6 0, :ic7 20
   :ic8 20, :ic9 20, :ic10 10, :ic11 10, :ic12 20, :ic13 1, :ic14 17
   :is1 1, :is2 10, :is3 29, :is4 1, :is5 1, :is6 1, :is7 0})

(def sample-suite
  (into (sorted-map)
        (map (fn [[query params]] [query [params]]))
        sample-params))


;; ============================================================
;; Database connection
;; ============================================================

(defn get-conn
  "Get a connection to the LDBC SNB database."
  ([]
   (get-conn db-path))
  ([path]
   (d/get-conn path schema/schema schema/db-opts)))

(defn close-conn [conn]
  (d/close conn))

;; ============================================================
;; Data loading
;; ============================================================

(defn load-data
  "Load LDBC SNB data from the given directory."
  ([data-dir]
   (load-data db-path data-dir))
  ([target-db-path data-dir]
   (println "============================================")
   (println "LDBC SNB Data Loader for Datalevin")
   (println "============================================")
   (println)
   (println "Data directory:" data-dir)
   (println "Database path:" target-db-path)
   (println)

   (when-not (.exists (io/file data-dir))
     (throw (ex-info (str "Data directory does not exist: " data-dir)
                     {:data-dir data-dir})))

   ;; Load data using empty-db + fill-db pattern.
   (loader/load-all-data target-db-path data-dir)))

;; ============================================================
;; Benchmark execution
;; ============================================================

(defn run-query
  "Run a single query and return timing results."
  [db query-def params]
  (let [params (if-let [param-fn (:param-fn query-def)]
                 (param-fn params)
                 params)]
    (if-let [runner (:runner query-def)]
      (let [start-time (System/nanoTime)
            result-rows (vec (runner db params))
            result-count (count result-rows)
            end-time (System/nanoTime)
            exec-time (/ (- end-time start-time) 1000000.0)]
        {:name (:name query-def)
         :planning-time 0.0
         :execution-time exec-time
         :result-count result-count
         :rows result-rows
         :columns (:columns query-def)})
      (let [query (:query query-def)
            rules (:rules query-def)
            post-process (:post-process query-def)
            param-vals (map #(get params %) (:params query-def))
            ;; Build query inputs
            inputs (if rules
                     (concat [db rules] param-vals)
                     (concat [db] param-vals))
            start-time (System/nanoTime)
            rows (apply d/q query inputs)
            final-rows (if post-process
                         (post-process db params rows)
                         rows)
            result-rows (vec final-rows)
            result-count (count result-rows)
            end-time (System/nanoTime)
            exec-time (/ (- end-time start-time) 1000000.0)]
        {:name (:name query-def)
         :planning-time 0.0
         :execution-time exec-time
         :result-count result-count
         :rows result-rows
         :columns (query-find-columns query)}))))

(defn- normalize-query-names
  [names]
  (->> names
       (map str/trim)
       (remove str/blank?)
       (map str/upper-case)))

(defn run-all-queries
  "Run benchmark queries and collect results.
   When query-names is provided, only those queries are executed."
  ([db params]
   (run-all-queries db params nil))
  ([db params query-names]
   (let [all-query-defs (concat ic/all-queries is/all-queries)
         wanted (when (seq query-names)
                  (set (normalize-query-names query-names)))
         query-defs (if wanted
                      (filter #(contains? wanted (:name %)) all-query-defs)
                      all-query-defs)]
     (for [query-def query-defs]
       (let [param-key (keyword (str/lower-case (:name query-def)))
             query-params (get params param-key {})]
         (println "Running" (:name query-def) "-" (:description query-def))
         (try
           (let [result (run-query db query-def query-params)]
             (when (and show-results? (contains? result :rows))
               (println "  Results:")
               (if (seq (:rows result))
                 (doseq [row (:rows result)]
                   (println "   " (pr-str row)))
                 (println "   []")))
             result)
           (catch Exception e
             (when debug-errors?
               (println "  DEBUG: stack trace for" (:name query-def))
               (stacktrace/print-stack-trace e)
               (println))
             (println "  ERROR:" (.getMessage e))
             (throw (ex-info (str "Query " (:name query-def) " failed")
                             {:query (:name query-def)}
                             e)))))))))

(defn write-results-rows
  "Write query result rows to a Neo4j-style plain CSV file."
  [results results-path]
  (when-let [parent (.getParentFile (io/file results-path))]
    (.mkdirs parent))
  (with-open [w (io/writer results-path :encoding "UTF-8")]
    (doseq [r results]
      (let [rows (:rows r)
            columns (result-columns r)]
        (when (seq columns)
          (.write w (str "query, " (str/join ", " columns) "\n")))
        (when (seq rows)
          (doseq [row rows]
            (.write w (str (format-row (:name r) row) "\n"))))))))

(defn write-perf
  "Write benchmark timing results to CSV file."
  [results perf-path]
  (when-let [parent (.getParentFile (io/file perf-path))]
    (.mkdirs parent))
  (with-open [w (io/writer perf-path :encoding "UTF-8")]
    (.write w "Query,Total Time (ms),Result Count,Error\n")
    (doseq [r results]
      (let [has-error (some? (:error r))
            exec-time (if (number? (:execution-time r)) (:execution-time r) 0)
            count-val (if (number? (:result-count r)) (:result-count r) 0)]
        (if has-error
          (.write w (format "%s,,,%s\n"
                            (:name r)
                            (str/replace (str (:error r)) "," ";")))
          (.write w (format "%s,%.3f,%d,\n"
                            (:name r)
                            (double exec-time)
                            (long count-val))))))))

(defn print-summary
  "Print benchmark summary to console."
  [results]
  (println)
  (println "============================================")
  (println "Benchmark Results Summary")
  (println "============================================")
  (println)
  (println (format "%-8s %-35s %12s %8s"
                   "Query" "Description" "Total (ms)" "Results"))
  (println (apply str (repeat 80 "-")))

  (doseq [r results]
    (let [exec (if (number? (:execution-time r)) (:execution-time r) -1)
          cnt (if (number? (:result-count r)) (:result-count r) 0)]
      (println (format "%-8s %-35s %12.3f %8d"
                       (:name r)
                       (or (:description r) "")
                       (double exec)
                       cnt))))

  (println (apply str (repeat 80 "-")))

  ;; Summary statistics
  (let [valid-results (filter #(and (number? (:execution-time %))
                                    (pos? (:execution-time %))) results)
        ic-results (filter #(str/starts-with? (:name %) "IC") valid-results)
        is-results (filter #(str/starts-with? (:name %) "IS") valid-results)]
    (println)
    (println "Interactive Complex (IC) queries:")
    (println (format "  Successful: %d / %d" (count ic-results) (count (filter #(str/starts-with? (:name %) "IC") results))))
    (when (seq ic-results)
      (println (format "  Avg Total Time: %.3f ms"
                       (/ (reduce + (map #(or (:execution-time %) 0) ic-results)) (count ic-results)))))

    (println)
    (println "Interactive Short (IS) queries:")
    (println (format "  Successful: %d / %d" (count is-results) (count (filter #(str/starts-with? (:name %) "IS") results))))
    (when (seq is-results)
      (println (format "  Avg Total Time: %.3f ms"
                       (/ (reduce + (map #(or (:execution-time %) 0) is-results)) (count is-results)))))))

(defn run-benchmark
  "Run the correctness-gated LDBC SNB read-query latency harness."
  ([] (run-benchmark {}))
  ([opts]
   (let [opts (if (sequential? opts)
                {:query-names opts}
                opts)
         opts (-> (merge default-bench-options opts)
                  (assoc :debug? debug-errors?)
                  (update :show-results? #(or show-results? %)))]
     (harness/run-benchmark!
       {:all-query-defs (vec (concat ic/all-queries is/all-queries))
        :sample-suite sample-suite
        :sample-result-counts sample-result-counts
        :get-conn get-conn
        :close-conn close-conn
        :run-query run-query
        :before-query
        (fn [phase _]
          (when (= phase :measurement)
            (.clear ^LRUCache query/*query-cache*)
            (.clear ^LRUCache query/*plan-cache*)))
        :measurement-cache-policy :fresh-parse-and-plan
        :report-extra
        {:datalevin-benchmark
         {:measurement-cache-implementation
          :clear-parse-and-plan-caches-before-each-query}}
        :write-results-rows write-results-rows}
       opts))))

;; ============================================================
;; CLI entry point
;; ============================================================

(defn- parse-bench-args
  "Parse benchmark command arguments."
  [args]
  (harness/parse-bench-args args))

(defn -main
  "Main entry point for loading data and running the read-query harness."
  [& args]
  (try
    (let [cmd (first args)]
      (case cmd
        "load"
        (if (some #{"--help"} (rest args))
          (println (harness/usage))
          (let [{:keys [db-path data-path]}
                (harness/parse-load-args (rest args))]
            (load-data db-path data-path)))

        "bench"
        (let [opts (parse-bench-args (rest args))]
          (if (:help? opts)
            (println (harness/usage))
            (let [report (run-benchmark opts)]
              (when (pos? (:exit-code report))
                (throw (ex-info "One or more benchmark queries failed"
                                {:exit-code (:exit-code report)}))))))

        (if (or (nil? cmd) (= "--help" cmd))
          (println (harness/usage))
          (throw (ex-info (str "Unknown command: " cmd)
                          {:command cmd})))))
    (finally
      (shutdown-agents))))

;; ============================================================
;; REPL helpers
;; ============================================================

(comment
  ;; Load data
  (load-data "data")

  ;; Run benchmark
  (run-benchmark)

  ;; Interactive exploration
  (def conn (get-conn))
  (def db (d/db conn))

  (binding [datalevin.query/*debug-plan* true]
    (let [{:keys [person-id min-date]} (:ic5 sample-params)]
      (d/explain {:run? true} (:query ic/ic5)
                 db person-id min-date)))



  ;; Count entities
  (d/q '[:find (count ?p) :where [?p :person/id _]] db)
  (d/q '[:find (count ?m) :where [?m :message/id _]] db)

  ;; Sample person
  (d/q '[:find ?id ?first ?last
         :where
         [?p :person/id ?id]
         [?p :person/firstName ?first]
         [?p :person/lastName ?last]
         :limit 5]
       db)

  ;; Close connection
  (close-conn conn)
  )
