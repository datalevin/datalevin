(ns ycsb-bench.core
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [datalevin-bench.host :as host]
            [datalevin.constants :as c]
            [ycsb-bench.runner :as runner]
            [ycsb-bench.sql :as sql]))

(defn- choice [s] (keyword (str/lower-case s)))

(defn- client-counts [s] (mapv parse-long (str/split s #",")))

(def cli-options
  [[nil "--system NAME" "datalevin, sqlite, postgres, all (default datalevin)" :parse-fn choice]
   [nil "--api API" "kv, datalog, all (default all)" :parse-fn choice]
   [nil "--mode MODE" "embedded, remote, all (default all)" :parse-fn choice]
   [nil "--workload NAME" "A, B, C, D, E, F, all (default A)" :parse-fn choice]
   [nil "--records N" "Initial records (10000)" :parse-fn parse-long]
   [nil "--ops N" "Measured operations, total across workers (10000)" :parse-fn parse-long]
   [nil "--warmup N" "Warmup operations, total across workers (1000)" :parse-fn parse-long]
   [nil "--threads N" "Concurrent worker threads (1)" :parse-fn parse-long]
   [nil "--pool-size N" "Remote Datalevin pool size (threads); SQL has one connection per worker" :parse-fn parse-long]
   [nil "--client-counts N,..." "Run matching worker/pool counts, e.g. 1,2,4,8,16" :parse-fn client-counts]
   [nil "--datalog-handles MODE" "Remote Datalog: shared, independent, both (independent)" :parse-fn choice]
   [nil "--repetitions N" "Fresh trials, reversing case order each trial (1)" :parse-fn parse-long]
   [nil "--warmup-ms N" "Timed warmup; overrides --warmup" :parse-fn parse-long]
   [nil "--measurement-ms N" "Timed measurement; overrides --ops" :parse-fn parse-long]
   [nil "--server-mode MODE" "process or in-process (process)" :parse-fn choice]
   [nil "--server-heap-mb N" "Separate server's fixed JVM heap (4096)" :parse-fn parse-long]
   [nil "--server-transaction-threads N" "Server concurrent explicit transaction limit (16)" :parse-fn parse-long]
   [nil "--server-background-threads N" "Fixed server background threads (4)" :parse-fn parse-long]
   [nil "--server-transaction-lock-timeout-ms N" "Server write-slot timeout (1000)" :parse-fn parse-long]
   [nil "--server-startup-timeout-ms N" "Separate server readiness timeout (120000)" :parse-fn parse-long]
   [nil "--seed N" "Random seed (17)" :parse-fn parse-long]
   [nil "--field-count N" "Fields per record (10)" :parse-fn parse-long]
   [nil "--field-length N" "ASCII bytes per field (100)" :parse-fn parse-long]
   [nil "--scan-length N" "Maximum scan length; uniform 1..N (100)" :parse-fn parse-long]
   [nil "--batch-size N" "Datalevin records per load transaction (1); SQL commits each insert" :parse-fn parse-long]
   [nil "--distribution NAME" "uniform, zipfian, latest (workload default)" :parse-fn choice]
   [nil "--zipfian-keyspace N" "Fixed scrambled keyspace; default predicts inserts with 2x headroom" :parse-fn parse-long]
   [nil "--durability NAME" "WAL profile: strict, relaxed (strict)" :parse-fn choice]
   [nil "--timeout-ms N" "Remote request / SQL timeout (60000)" :parse-fn parse-long]
   [nil "--phase-timeout-ms N" "Cancel an overdue phase after N ms (600000)" :parse-fn parse-long]
   [nil "--pg-url URL" "PostgreSQL JDBC URL (YCSB_PG_URL or localhost:5432/postgres)"]
   [nil "--pg-user USER" "PostgreSQL user (YCSB_PG_USER or driver default)"]
   [nil "--keep-db" "Keep generated databases / PostgreSQL schemas" :id :keep-db?]
   [nil "--value-audit" "Record and check values; timings include audit overhead" :id :value-audit?]
   [nil "--output FILE" "Write a complete EDN report after successful validation"]
   ["-h" "--help" "Show usage"]])

(defn summarize-trials
  "Group identical effective configurations, excluding the trial number, and
  report median and spread without pooling latencies."
  [results]
  (mapv
    (fn [[configuration trials]]
      (let [rates (vec (sort (map #(get-in % [:measured :ops-per-second]) trials)))
            n (count rates)
            mid (quot n 2)
            median (if (odd? n) (nth rates mid)
                       (/ (+ (double (nth rates (dec mid))) (double (nth rates mid))) 2.0))]
        {:configuration configuration :trials n
         :ops-per-second {:median median :min (first rates) :max (peek rates)}}))
    (sort-by (comp pr-str key)
             (group-by #(dissoc (:configuration %) :trial)
                       results))))

(defn run-benchmark
  "Run the selected cases, each with a fresh database. Returns an EDN report."
  [provided]
  (let [opts (runner/options provided)
        cases (runner/cases opts)]
    (when (some #(= :postgres (:system %)) cases) (sql/check-postgres! opts))
    (host/with-paused-media
      (let [report
            {:format-version 3
             :benchmark :datalevin-ycsb-style
             :datalevin-version c/version
             :measurement-model :closed-loop
             :run-options (dissoc opts :pg-url :pg-user)
             :started-at (str (java.time.Instant/now))
             :environment {:java (System/getProperty "java.version")
                           :os (System/getProperty "os.name")
                           :arch (System/getProperty "os.arch")
                           :max-heap-bytes (.maxMemory (Runtime/getRuntime))
                           :processors (.availableProcessors (Runtime/getRuntime))}
             :results
             (vec
               (for [{:keys [system api mode workload threads datalog-handles trial] :as case-opts} cases]
                 (let [result (runner/run-case! case-opts)
                       measured (:measured result)]
                   (println (format "%s %s %s %s: %.1f ops/s, p99 %.1f us, %d records checked (%s) [trial %d, %d workers%s]"
                                    (name system) (name api) (name mode) (str/upper-case (name workload))
                                    (:ops-per-second measured) (get-in measured [:latency-us :p99])
                                    (get-in result [:validation :records])
                                    (if (:value-audit? case-opts)
                                      "value audit; timings include recording" "structure only")
                                    trial threads
                                    (if (and (= system :datalevin) (= api :datalog) (= mode :remote))
                                      (str ", " (name datalog-handles) " handles") "")))
                   result)))}]
        (assoc report :summary (summarize-trials (:results report)))))))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (try
      (cond
        (:help options) (println (str "YCSB-style Datalevin benchmark\n\n" summary))
        (or (seq errors) (seq arguments))
        (throw (ex-info "Invalid command line" {:errors errors :arguments arguments}))
        :else
        (let [report (run-benchmark (dissoc options :output :help))]
          (if-let [output (:output options)]
            (do (spit output (with-out-str (pp/pprint report)))
                (println "Report:" output))
            (pp/pprint report))))
      (finally (shutdown-agents)))))
