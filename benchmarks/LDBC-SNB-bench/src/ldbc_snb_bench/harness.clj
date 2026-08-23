(ns ldbc-snb-bench.harness
  "Reproducible query-latency harness for LDBC SNB Interactive v1 reads.

   This is deliberately not an implementation of the official LDBC driver:
   it does not schedule updates, dependent short reads, or throughput streams."
  (:require
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [datalevin.constants :as constants]
   [datalevin.core :as d]
   [datalevin.query :as q]
   [ldbc-snb-bench.parameters :as parameters])
  (:import
   [java.io File]
   [java.lang ProcessHandle]
   [java.nio.charset StandardCharsets]
   [java.security MessageDigest]
   [java.time Instant]
   [java.util ArrayList Date Locale Random UUID]))

(def ^:private jvm-instance-id
  (str (UUID/randomUUID)))

(def default-options
  {:db-path "db/ldbc-snb"
   :results-path "results/results.csv"
   :perf-path "results/perf.csv"
   :output "results/report.edn"
   :warmup 0
   :iterations 1
   :run-role :measurement
   :parameter-count 10
   :seed 42
   :scale-factor "1"
   :verify? true
   :query-cache? false
   :show-results? false})

(defn normalize-query-names
  [names]
  (->> names
       (map str)
       (map str/trim)
       (remove str/blank?)
       (map str/upper-case)
       distinct
       vec))

(defn select-query-defs
  [all-query-defs names]
  (let [by-name (into {} (map (juxt :name identity)) all-query-defs)
        wanted  (normalize-query-names names)
        unknown (remove #(contains? by-name %) wanted)]
    (when (seq unknown)
      (throw (ex-info (str "Unknown query name(s): " (str/join ", " unknown)
                           ". Valid names: "
                           (str/join ", " (map :name all-query-defs)))
                      {:unknown (vec unknown)})))
    (if (seq wanted)
      (mapv by-name wanted)
      (vec all-query-defs))))

;; ---------------------------------------------------------------------------
;; Stable digests and latency statistics
;; ---------------------------------------------------------------------------

(declare canonical-value)

(defn- canonical-map
  [value]
  (into (sorted-map-by #(compare (pr-str %1) (pr-str %2)))
        (map (fn [[key v]] [(canonical-value key) (canonical-value v)]))
        value))

(defn canonical-value
  "Convert query results to a deterministic, EDN-printable representation."
  [value]
  (cond
    (nil? value) nil
    (instance? Date value) [:date-millis (.getTime ^Date value)]
    (instance? Instant value) [:instant-millis (.toEpochMilli ^Instant value)]
    (map? value) (canonical-map value)
    (set? value) (->> value
                      (map canonical-value)
                      (sort-by pr-str)
                      vec)
    (and value (.isArray (class value)))
    (mapv canonical-value (seq value))
    (sequential? value) (mapv canonical-value value)
    :else value))

(defn sha256
  [value]
  (let [bytes  (.getBytes (pr-str (canonical-value value))
                          StandardCharsets/UTF_8)
        digest (.digest (MessageDigest/getInstance "SHA-256") bytes)]
    (apply str (map #(format "%02x" (bit-and (int %) 0xff)) digest))))

(defn result-digest
  [rows]
  (sha256 rows))

(defn percentile
  "Nearest-rank percentile over a non-empty sorted collection."
  [sorted-values fraction]
  (let [n     (count sorted-values)
        index (min (dec n)
                   (dec (long (Math/ceil (* fraction n)))))]
    (nth sorted-values (max 0 index))))

(defn latency-summary
  [times-ms]
  (let [values (vec (sort times-ms))
        n      (count values)]
    (when (pos? n)
      {:count n
       :min (first values)
       :median (percentile values 0.5)
       :p95 (percentile values 0.95)
       :p99 (percentile values 0.99)
       :max (peek values)
       :mean (/ (reduce + values) (double n))})))

;; ---------------------------------------------------------------------------
;; Parameter sources and deterministic schedules
;; ---------------------------------------------------------------------------

(defn- short-query-key?
  [query]
  (str/starts-with? (name query) "is"))

(defn prepare-parameter-source
  [parameter-path selected-query-defs sample-suite]
  (let [selected-keys (mapv #(parameters/query-key (:name %))
                            selected-query-defs)
        sample-origin (fn [_]
                        {:kind :bundled-sf1-sample})
        source (if parameter-path
                 (parameters/load-source parameter-path)
                 {:kind :bundled-sf1-sample
                  :parameters sample-suite
                  :origins (into {}
                                 (map (juxt identity sample-origin))
                                 (keys sample-suite))})
        ;; Datagen v1 substitution files cover IC1-IC14. Short reads in the
        ;; official workload are dependent operations, so this latency harness
        ;; retains its bundled IS parameters unless a complete EDN suite is used.
        source (if (= :official-directory (:kind source))
                 (let [short-keys (filter short-query-key? selected-keys)]
                   (-> source
                       (update :parameters merge
                               (select-keys sample-suite short-keys))
                       (update :origins merge
                               (into {}
                                     (map (juxt identity sample-origin))
                                     short-keys))))
                 source)
        missing (remove #(seq (get-in source [:parameters %])) selected-keys)]
    (when (seq missing)
      (throw (ex-info
               (str "The parameter source has no rows for: "
                    (str/join ", "
                              (map #(str/upper-case (name %)) missing)))
               {:parameter-source parameter-path
                :missing (vec missing)})))
    (assoc source
           :available-counts
           (into (sorted-map)
                 (map (fn [[query rows]] [query (count rows)]))
                 (:parameters source)))))

(defn- deterministic-selection
  [rows n seed]
  (let [items  (ArrayList.)
        random (Random. (long seed))]
    (doseq [[source-index params] (map-indexed vector rows)]
      (.add items {:source-index source-index :params params}))
    ;; Spell out Fisher-Yates so the schedule does not depend on a JDK
    ;; Collections implementation detail.
    (loop [index (dec (.size items))]
      (when (pos? index)
        (let [other (.nextInt random (inc index))
              value (.get items index)]
          (.set items index (.get items other))
          (.set items other value)
          (recur (dec index)))))
    (vec (take (min n (.size items)) items))))

(defn- sf1?
  [scale-factor]
  (try
    (= 1.0 (Double/parseDouble (str scale-factor)))
    (catch NumberFormatException _ false)))

(defn build-schedule
  [all-query-defs selected-query-defs source
   {:keys [parameter-count seed scale-factor]} sample-result-counts]
  (let [ordinals (into {} (map-indexed (fn [index query-def]
                                         [(:name query-def) index])
                                       all-query-defs))]
    (->> selected-query-defs
         (mapcat
           (fn [query-def]
             (let [query-key   (parameters/query-key (:name query-def))
                   rows        (get-in source [:parameters query-key])
                   query-seed  (+ (long seed)
                                  (* 1000003
                                     (inc (long (get ordinals
                                                     (:name query-def))))))
                   selected    (deterministic-selection rows parameter-count
                                                        query-seed)
                   origin      (get-in source [:origins query-key])
                   oracle?     (and (sf1? scale-factor)
                                    (= :bundled-sf1-sample (:kind origin)))]
               (map-indexed
                 (fn [selection-index {:keys [source-index params]}]
                   {:query-def query-def
                    :query-key query-key
                    :selection-index selection-index
                    :source-index source-index
                    :params params
                    :origin origin
                    :expected-count (when oracle?
                                      (get sample-result-counts query-key))})
                 selected))))
         vec)))

(defn schedule-artifact
  [source schedule]
  (let [public-entry (fn [{:keys [query-key selection-index source-index
                                  params origin expected-count]}]
                       (cond-> {:query query-key
                                :selection-index selection-index
                                :source-index source-index
                                :parameters params
                                :origin origin}
                         (some? expected-count)
                         (assoc :expected-count expected-count)))
        public-schedule (mapv public-entry schedule)]
    {:source (cond-> {:kind (:kind source)
                     :available-counts (:available-counts source)
                     :origins (:origins source)}
              (:path source) (assoc :path (:path source)))
     :selected-counts (into (sorted-map)
                            (map (fn [[query entries]]
                                   [query (count entries)]))
                            (group-by :query-key schedule))
     :sha256 (sha256 public-schedule)
     :entries public-schedule}))

;; ---------------------------------------------------------------------------
;; Correctness-gated repeated execution
;; ---------------------------------------------------------------------------

(def ^:dynamic *benchmark-phase*
  "Current suite phase, exposed so runners can bypass plan/query caches only
   for measured executions."
  nil)

(defn- exception-data
  [^Exception exception]
  {:class (.getName (class exception))
   :message (or (.getMessage exception) (str exception))})

(defn- observation
  [result]
  {:time-ms (double (:execution-time result))
   :result-count (long (:result-count result))
   :result-sha256 (result-digest (:rows result))})

(defn- execute-once
  [run-once params debug?]
  (try
    (let [result (run-once params)]
      {:status :ok
       :result result
       :observed (observation result)})
    (catch Exception exception
      (when debug?
        (.printStackTrace exception))
      {:status :error
       :error (exception-data exception)})))

(defn- entry-base
  [{:keys [query-def query-key selection-index source-index params origin]}]
  {:name (:name query-def)
   :query query-key
   :description (:description query-def)
   :selection-index selection-index
   :source-index source-index
   :parameters params
   :parameter-origin origin})

(defn- initial-state
  [entry]
  {:entry entry
   :base (entry-base entry)
   :status :active
   :baseline nil
   :output nil
   :observations []
   :execution-count 0
   :verified-comparisons 0})

(defn- remember-execution
  [state result observed]
  (cond-> (update state :execution-count inc)
    (nil? (:baseline state)) (assoc :baseline observed)
    (nil? (:output state)) (assoc :output result)))

(defn- process-execution
  [state phase iteration verify? execution]
  (if (= :error (:status execution))
    (assoc state
           :status :failed
           :failure {:status :error
                     :phase phase
                     :iteration iteration
                     :error (:error execution)})
    (let [{:keys [result observed]} execution
          expected-count (get-in state [:entry :expected-count])
          expected-digest (get-in state [:baseline :result-sha256])
          state' (remember-execution state result observed)]
      (cond
        (and verify?
             (some? expected-count)
             (not= expected-count (:result-count observed)))
        (assoc state'
               :status :failed
               :failure {:status :incorrect
                         :phase phase
                         :iteration iteration
                         :oracle :sf1-result-count
                         :expected-result-count expected-count
                         :observed observed})

        (and verify?
             expected-digest
             (not= expected-digest (:result-sha256 observed)))
        (assoc state'
               :status :failed
               :failure {:status :incorrect
                         :phase phase
                         :iteration iteration
                         :oracle :repeat-digest
                         :expected-result-sha256 expected-digest
                         :observed observed})

        :else
        (cond-> state'
          (= phase :measurement)
          (update :observations conj observed)

          (and verify? expected-digest)
          (update :verified-comparisons inc))))))

(defn- execute-round
  [states phase iteration run-entry
   {:keys [before-query progress-fn verify? debug?]}]
  (mapv
    (fn [{:keys [entry status] :as state}]
      (if (not= :active status)
        state
        (do
          (when progress-fn
            (progress-fn phase iteration entry))
          (process-execution
            state phase iteration verify?
            (execute-once
              (fn [_]
                (when before-query
                  (before-query phase entry))
                (binding [*benchmark-phase* phase]
                  (run-entry entry)))
              (:params entry)
              debug?)))))
    states))

(defn- execute-rounds
  [states phase n run-entry opts]
  (loop [iteration 0
         current states]
    (if (= iteration n)
      current
      (recur (inc iteration)
             (execute-round current phase iteration run-entry opts)))))

(defn- failure-correctness
  [{:keys [status oracle expected-result-count]}]
  (if (= :incorrect status)
    (cond-> {:status :failed :oracle oracle}
      (some? expected-result-count)
      (assoc :expected-count expected-result-count))
    {:status :not-completed}))

(defn- finalize-failed-state
  [{:keys [base failure baseline output observations]}]
  (cond-> (merge base
                 (select-keys failure
                              [:status :phase :iteration :error
                               :expected-result-count
                               :expected-result-sha256 :observed])
                 {:samples-ms (mapv :time-ms observations)
                  :correctness (failure-correctness failure)})
    (or baseline output)
    (assoc :result-count (or (:result-count baseline)
                             (:result-count output))
           :result-sha256 (or (:result-sha256 baseline)
                              (some-> output :rows result-digest))
           :rows (:rows output)
           :columns (:columns output))))

(defn- successful-correctness
  [{:keys [entry verified-comparisons]} verify?]
  (let [expected-count (:expected-count entry)]
    (cond
      (not verify?)
      {:status :skipped}

      (some? expected-count)
      {:status :passed
       :oracle (if (pos? verified-comparisons)
                 :sf1-count-and-repeat-digest
                 :sf1-result-count)
       :expected-count expected-count}

      (pos? verified-comparisons)
      {:status :consistent
       :oracle :repeat-digest}

      :else
      {:status :single-execution
       :oracle :none})))

(defn- finalize-successful-state
  [{:keys [base baseline output observations] :as state} verify?]
  (let [samples (mapv :time-ms observations)
        summary (latency-summary samples)]
    (merge base
           {:status :ok
            :phase :complete
            :result-count (:result-count baseline)
            :result-sha256 (:result-sha256 baseline)
            :execution-time (:median summary)
            :latency-ms summary
            :samples-ms samples
            :rows (:rows output)
            :columns (:columns output)
            :correctness (successful-correctness state verify?)})))

(defn benchmark-schedule
  "Execute complete warmup passes before complete measured passes.

   The first successful execution establishes the correctness baseline. With
   the default zero in-process warmups, this is the measured execution itself."
  [schedule run-entry {:keys [warmup iterations verify?] :as opts}]
  (let [states (mapv initial-state schedule)
        warmed (execute-rounds states :warmup warmup run-entry opts)
        measured (execute-rounds warmed :measurement iterations run-entry opts)]
    (mapv (fn [{:keys [status] :as state}]
            (if (= :failed status)
              (finalize-failed-state state)
              (finalize-successful-state state verify?)))
          measured)))

(defn benchmark-parameter
  "Benchmark one query/parameter pair using warmup then measured execution."
  [entry run-once opts]
  (first
    (benchmark-schedule
      [entry]
      (fn [{:keys [params]}] (run-once params))
      opts)))

(defn result-failure?
  [{:keys [status]}]
  (contains? #{:error :incorrect} status))

;; ---------------------------------------------------------------------------
;; Reporting
;; ---------------------------------------------------------------------------

(defn host-info
  []
  {:timestamp (str (Instant/now))
   :process-id (.pid (ProcessHandle/current))
   :jvm-instance-id jvm-instance-id
   :clojure (clojure-version)
   :clojure-direct-linking
   (Boolean/parseBoolean
     (System/getProperty "clojure.compiler.direct-linking" "false"))
   :datalevin constants/version
   :java (System/getProperty "java.version")
   :vm (System/getProperty "java.vm.name")
   :os (System/getProperty "os.name")
   :os-version (System/getProperty "os.version")
   :arch (System/getProperty "os.arch")
   :processors (.availableProcessors (Runtime/getRuntime))
   :max-heap-bytes (.maxMemory (Runtime/getRuntime))})

(defn db-manifest
  [path]
  (let [root  (io/file path)
        files (->> (file-seq root)
                   (filter #(and (.isFile ^File %)
                                 (not= "lock.mdb" (.getName ^File %))))
                   (map (fn [^File file]
                          {:path (str (.relativize (.toPath root)
                                                 (.toPath file)))
                           :bytes (.length file)
                           :last-modified-ms (.lastModified file)}))
                   (sort-by :path)
                   vec)]
    {:path (.getCanonicalPath ^File root)
     :file-count (count files)
     :total-bytes (reduce + 0 (map :bytes files))
     :metadata-sha256 (sha256 files)
     :excluded-files ["lock.mdb"]
     :files files}))

(defn- fixed
  [value]
  (when (number? value)
    (String/format Locale/ROOT "%.3f" (object-array [(double value)]))))

(defn- csv-cell
  [value]
  (let [value (str (or value ""))]
    (if (re-find #"[\",\r\n]" value)
      (str "\"" (str/replace value "\"" "\"\"") "\"")
      value)))

(defn- ensure-parent!
  [path]
  (when-let [parent (.getParentFile ^File (io/file path))]
    (.mkdirs parent)))

(defn write-perf!
  [results path]
  (ensure-parent! path)
  (with-open [writer (io/writer path :encoding "UTF-8")]
    (.write writer
            (str "Query,Parameter Index,Source Index,Samples,Min (ms),"
                 "Median (ms),P95 (ms),P99 (ms),Max (ms),Mean (ms),"
                 "Result Count,Result SHA-256,Status,Error\n"))
    (doseq [result results]
      (let [latency (:latency-ms result)
            row [(:name result)
                 (:selection-index result)
                 (:source-index result)
                 (count (:samples-ms result))
                 (fixed (:min latency))
                 (fixed (:median latency))
                 (fixed (:p95 latency))
                 (fixed (:p99 latency))
                 (fixed (:max latency))
                 (fixed (:mean latency))
                 (:result-count result)
                 (:result-sha256 result)
                 (some-> (:status result) name)
                 (get-in result [:error :message])]]
        (.write writer (str (str/join "," (map csv-cell row)) "\n"))))))

(defn query-summaries
  [results]
  (let [grouped (group-by :name results)]
    (mapv
      (fn [query]
        (let [query-results (get grouped query)
              failure (first (filter result-failure? query-results))
              samples (mapcat :samples-ms
                              (remove result-failure? query-results))
              counts  (sort (distinct (keep :result-count query-results)))]
          (cond-> {:query query
                   :parameter-count (count query-results)
                   :sample-count (count samples)
                   :status (or (:status failure) :ok)
                   :result-counts (vec counts)}
            (seq samples) (assoc :latency-ms (latency-summary samples))
            failure (assoc :failure (select-keys failure
                                                 [:selection-index :phase
                                                  :status :error])))))
      (distinct (map :name results)))))

(defn- result-count-label
  [counts]
  (cond
    (empty? counts) "-"
    (= 1 (count counts)) (str (first counts))
    :else (str (first counts) ".." (last counts))))

(defn print-summary!
  [summaries]
  (println)
  (println "================================================================================")
  (println "LDBC SNB read-query latency summary")
  (println "================================================================================")
  (println (format "%-7s %7s %8s %12s %12s %12s %10s %10s"
                   "Query" "Params" "Samples" "Median ms" "P95 ms"
                   "P99 ms" "Results" "Status"))
  (println (apply str (repeat 90 "-")))
  (doseq [{:keys [query parameter-count sample-count status result-counts
                  latency-ms]} summaries]
    (println
      (format "%-7s %7d %8d %12s %12s %12s %10s %10s"
              query
              parameter-count
              sample-count
              (or (fixed (:median latency-ms)) "-")
              (or (fixed (:p95 latency-ms)) "-")
              (or (fixed (:p99 latency-ms)) "-")
              (result-count-label result-counts)
              (name status))))
  (println (apply str (repeat 90 "-"))))

(defn write-edn!
  [path value]
  (ensure-parent! path)
  (spit path (with-out-str (pprint/pprint value)) :encoding "UTF-8"))

(def report-configuration-keys
  [:warmup :iterations :parameter-count :seed :scale-factor :verify?
   :query-cache? :query-names :run-role :results-path :perf-path :output])

(defn run-benchmark!
  [{:keys [all-query-defs sample-suite sample-result-counts get-conn
           close-conn run-query write-results-rows connection->db manifest-fn
           system-name report-extra before-query measurement-cache-policy]}
   raw-options]
  (let [opts       (merge default-options raw-options)
        system-name (or system-name "Datalevin")
        connection->db (or connection->db d/db)
        manifest-fn (or manifest-fn db-manifest)
        query-defs (select-query-defs all-query-defs (:query-names opts))
        db-path    (:db-path opts)
        db-file    (io/file db-path)]
    (when-not (.exists ^File db-file)
      (throw (ex-info (str "Database does not exist: " db-path
                           ". Run the load command first.")
                      {:db-path db-path})))
    (let [source   (prepare-parameter-source (:parameters opts) query-defs
                                             sample-suite)
          schedule (build-schedule all-query-defs query-defs source opts
                                   sample-result-counts)
          schedule-report (schedule-artifact source schedule)
          manifest (manifest-fn db-path)]
      (println "============================================")
      (println "LDBC SNB read-query benchmark for" system-name)
      (println "============================================")
      (println "Database:" (:path manifest))
      (println "Parameter schedule SHA-256:" (:sha256 schedule-report))
      (println "Run role:" (name (:run-role opts)))
      (println "Query result cache:"
               (if (:query-cache? opts) "enabled" "disabled"))
      (when measurement-cache-policy
        (println "Measured query-cache policy:" measurement-cache-policy))
      (println)
      (let [connection (get-conn db-path)]
        (try
          (let [db      (connection->db connection)
                current-pass (atom nil)
                progress-fn
                (fn [phase iteration
                     {:keys [query-def selection-index source-index]}]
                  (let [pass [phase iteration]]
                    (when-not (= pass @current-pass)
                      (reset! current-pass pass)
                      (println
                        (str (if (= phase :warmup)
                               "In-process warmup"
                               (if (= :warmup (:run-role opts))
                                 "Warmup"
                                 "Measurement"))
                             " pass " (inc iteration) " of "
                             (if (= phase :warmup)
                               (:warmup opts)
                               (:iterations opts))))))
                  (println "  Running" (:name query-def)
                           (str "parameter " (inc selection-index)
                                " (source row " (inc source-index) ")")))
                results
                (benchmark-schedule
                  schedule
                  (fn [{:keys [query-def params]}]
                    (binding [q/*cache?* (:query-cache? opts)]
                      (run-query db query-def params)))
                  (assoc opts
                         :debug? (:debug? opts)
                         :before-query before-query
                         :progress-fn progress-fn))
                _ (doseq [result results]
                    (when (and (:show-results? opts) (seq (:rows result)))
                      (doseq [row (:rows result)]
                        (println "  " (pr-str row))))
                    (when (result-failure? result)
                      (println "  ERROR:"
                               (or (get-in result [:error :message])
                                   (str "correctness failure during "
                                        (name (:phase result)))))))
                summaries (query-summaries results)
                exit-code (if (some result-failure? results) 1 0)
                report-base
                {:format-version 1
                 :benchmark-suite :ldbc-snb-interactive-v1-read-latency
                 :benchmark-system system-name
                 :official-ldbc-result false
                 :host (host-info)
                 :dataset {:scale-factor (:scale-factor opts)
                           :database manifest}
                 :configuration (select-keys opts report-configuration-keys)
                 :timing-boundary
                 {:included [:query-execution :post-processing
                             :result-realization]
                  :excluded [:database-open :warmup :artifact-writing]
                  :query-result-cache (:query-cache? opts)
                  :measurement-cache-policy measurement-cache-policy}
                 :parameter-schedule schedule-report
                 :summaries summaries
                 :results (mapv #(dissoc % :rows) results)
                 :exit-code exit-code}
                extra (cond
                        (fn? report-extra) (report-extra connection)
                        (map? report-extra) report-extra
                        :else {})
                report (merge report-base extra)]
            (when-let [output (:output opts)]
              (write-edn! output report))
            (write-results-rows results (:results-path opts))
            (write-perf! results (:perf-path opts))
            (print-summary! summaries)
            (println)
            (println "Query outputs:" (:results-path opts))
            (println "Latency CSV:" (:perf-path opts))
            (when-let [output (:output opts)]
              (println "Raw EDN report:" output))
            report)
          (finally
            (close-conn connection)))))))

;; ---------------------------------------------------------------------------
;; CLI
;; ---------------------------------------------------------------------------

(def ^:private value-options
  #{"-o" "--results" "-p" "--perf" "--output" "--db"
    "--parameters" "--parameter-count" "--warmup" "--iterations"
    "--seed" "--scale-factor" "--run-role"})

(defn- require-option-values!
  [args]
  (loop [remaining args]
    (when-let [arg (first remaining)]
      (if (contains? value-options arg)
        (do
          (when-not (second remaining)
            (throw (ex-info (str "Missing value for " arg) {:option arg})))
          (recur (nnext remaining)))
        (recur (next remaining))))))

(defn- parse-long-option
  [option value allow-zero?]
  (try
    (let [parsed (Long/parseLong value)]
      (when (if allow-zero? (neg? parsed) (not (pos? parsed)))
        (throw (ex-info
                 (str option (if allow-zero?
                               " must not be negative"
                               " must be positive"))
                 {:option option :value value})))
      parsed)
    (catch NumberFormatException cause
      (throw (ex-info (str "Invalid integer for " option ": " value)
                      {:option option :value value}
                      cause)))))

(defn parse-bench-args
  [args]
  (require-option-values! args)
  (loop [remaining args
         opts default-options
         query-names []]
    (if-let [arg (first remaining)]
      (case arg
        "-o" (recur (nnext remaining)
                     (assoc opts :results-path (second remaining)) query-names)
        "--results" (recur (nnext remaining)
                            (assoc opts :results-path (second remaining))
                            query-names)
        "-p" (recur (nnext remaining)
                     (assoc opts :perf-path (second remaining)) query-names)
        "--perf" (recur (nnext remaining)
                         (assoc opts :perf-path (second remaining)) query-names)
        "--output" (recur (nnext remaining)
                           (assoc opts :output (second remaining)) query-names)
        "--db" (recur (nnext remaining)
                       (assoc opts :db-path (second remaining)) query-names)
        "--parameters" (recur (nnext remaining)
                               (assoc opts :parameters (second remaining))
                               query-names)
        "--parameter-count"
        (recur (nnext remaining)
               (assoc opts :parameter-count
                      (parse-long-option arg (second remaining) false))
               query-names)
        "--warmup"
        (recur (nnext remaining)
               (assoc opts :warmup
                      (parse-long-option arg (second remaining) true))
               query-names)
        "--iterations"
        (recur (nnext remaining)
               (assoc opts :iterations
                      (parse-long-option arg (second remaining) false))
               query-names)
        "--seed"
        (recur (nnext remaining)
               (assoc opts :seed
                      (parse-long-option arg (second remaining) true))
               query-names)
        "--scale-factor" (recur (nnext remaining)
                                 (assoc opts :scale-factor (second remaining))
                                 query-names)
        "--run-role"
        (let [role (keyword (str/lower-case (second remaining)))]
          (when-not (contains? #{:warmup :measurement} role)
            (throw (ex-info "--run-role must be warmup or measurement"
                            {:option arg :value (second remaining)})))
          (recur (nnext remaining) (assoc opts :run-role role) query-names))
        "--no-verify" (recur (next remaining)
                              (assoc opts :verify? false) query-names)
        "--query-cache" (recur (next remaining)
                                (assoc opts :query-cache? true) query-names)
        "--show-results" (recur (next remaining)
                                 (assoc opts :show-results? true) query-names)
        "--help" (recur (next remaining)
                         (assoc opts :help? true) query-names)
        (if (str/starts-with? arg "-")
          (throw (ex-info (str "Unrecognized option: " arg)
                          {:option arg}))
          (recur (next remaining) opts (conj query-names arg))))
      (assoc opts :query-names (normalize-query-names query-names)))))

(defn parse-load-args
  [args]
  (require-option-values! args)
  (loop [remaining args
         opts {:db-path (:db-path default-options)
               :data-path "data"}
         data-path-seen? false]
    (if-let [arg (first remaining)]
      (case arg
        "--db" (recur (nnext remaining)
                       (assoc opts :db-path (second remaining))
                       data-path-seen?)
        (if (str/starts-with? arg "-")
          (throw (ex-info (str "Unrecognized load option: " arg)
                          {:option arg}))
          (if data-path-seen?
            (throw (ex-info "Only one data directory may be supplied"
                            {:argument arg}))
            (recur (next remaining) (assoc opts :data-path arg) true))))
      opts)))

(defn usage
  []
  (str
    "LDBC SNB read-query benchmark for Datalevin\n\n"
    "Usage:\n"
    "  clj -M -m ldbc-snb-bench.core load [--db PATH] [DATA_DIR]\n"
    "  clj -M -m ldbc-snb-bench.core bench [OPTIONS] [IC1 IS1 ...]\n\n"
    "Benchmark options:\n"
    "  --db PATH             Datalevin database (default db/ldbc-snb)\n"
    "  --parameters PATH     Official parameter directory or EDN suite\n"
    "  --parameter-count N   Deterministically select up to N rows/query (default 10)\n"
    "  --warmup N            Same-process warmup passes (default 0)\n"
    "  --iterations N         Measured passes (default 1)\n"
    "  --run-role ROLE        Label pass as warmup or measurement\n"
    "  --seed N               Parameter selection seed (default 42)\n"
    "  --scale-factor SF      Dataset scale factor recorded in report (default 1)\n"
    "  -o, --results PATH     Query outputs CSV\n"
    "  -p, --perf PATH        Latency summary CSV\n"
    "  --output PATH          Raw samples and run manifest EDN\n"
    "  --no-verify            Skip result count/digest checks\n"
    "  --query-cache          Include Datalevin's query-result cache\n"
    "  --show-results         Print result rows\n"
    "  --help                 Show this help\n\n"
    "This harness measures read-query latency. It is not an official LDBC result;\n"
    "the official driver also schedules updates, dependent reads, and throughput.\n"))
