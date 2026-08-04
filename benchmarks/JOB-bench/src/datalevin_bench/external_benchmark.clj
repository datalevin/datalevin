(ns datalevin-bench.external-benchmark
  "Controlled repeated Datalevin/PostgreSQL runs for the CIDR viability table."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin-bench.core :as job]
   [datalevin-bench.evaluation :as evaluation]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.parser :as dp]
   [datalevin.query :as q]
   [datalevin.util :as u])
  (:import
   [datalevin.utl LRUCache]
   [java.lang.management ManagementFactory]
   [java.sql Connection DriverManager SQLException]
   [java.time Instant]
   [java.util ArrayList Collections Random]))

(def timing-header
  ["Run" "Schedule Seed" "Position" "System" "Query Name"
   "Query Sample Seed" "Planning Time (ms)" "Execution Time (ms)"
   "Result Size" "Status" "Error"])

(def ^:private frozen-estimator
  {:direct-counts? true
   :query-sampling? true
   :estimator-policy :production
   :sample-size 1000
   :prior-size 100
   :variance-alpha 0.4
   :tail-weight 0.0
   :conservative-lower-bound? true})

(defn query-sample-seed
  "Derive the same deterministic per-query seed as the optimizer runner."
  ^long [^long pass-seed ^String query-name]
  (let [query-hash (long (.hashCode query-name))]
    (bit-and 281474976710655
             (bit-xor pass-seed
                      query-hash
                      (bit-shift-left query-hash 32)))))

(defn- query-name
  [query-symbol]
  (str/replace (name query-symbol) "q-" ""))

(defn query-order
  "Return the deterministic query-name order for one pass."
  [seed]
  (let [names (ArrayList.
                (mapv query-name
                      (sort-by query-name job/queries)))]
    (Collections/shuffle names (Random. (long seed)))
    (vec names)))

(defn- query-entries
  [pass-seed]
  (let [by-name (into {}
                      (map (juxt query-name identity))
                      job/queries)]
    (mapv (fn [name]
            {:name name
             :symbol (by-name name)
             :sample-seed (query-sample-seed pass-seed name)})
          (query-order pass-seed))))

(defn- query-value
  [query-symbol]
  (-> (ns-resolve 'datalevin-bench.core query-symbol) var-get))

(defn- query-with-timeout
  [query timeout-ms]
  (assoc (if (map? query) query (dp/query->map query))
         :timeout timeout-ms))

(defn- timeout-error?
  [throwable]
  (loop [e throwable]
    (when e
      (or (= :query/timeout (:type (ex-data e)))
          (= "57014" (when (instance? SQLException e)
                       (.getSQLState ^SQLException e)))
          (re-find #"took too long|canceling statement due to statement timeout"
                   (or (.getMessage e) ""))
          (recur (.getCause e))))))

(defn- datalevin-query!
  [db {:keys [symbol sample-seed]} timeout-ms]
  (binding [c/use-direct-predicate-counts? true
            c/use-query-local-sampling? true
            c/link-estimate-policy :production
            c/init-exec-size-threshold 1000
            c/link-estimate-prior-size 100
            c/link-estimate-var-alpha 0.4
            c/link-estimate-tail-weight 0.0
            c/link-estimate-conservative-lower-bound? true
            u/*reservoir-sampling-seed* sample-seed
            q/*cache?* false
            q/*plan-cache* (LRUCache. c/query-plan-cache-size)]
    (let [result (d/explain {:run? true :intermediate-counts? false}
                            (query-with-timeout
                              (query-value symbol)
                              timeout-ms)
                            db)]
      {:planning-ms (parse-double (:prepare-time result))
       :execution-ms (parse-double (:execution-time result))
       :result-size (:actual-result-size result)
       :status :ok})))

(defn- sql-files
  []
  (into {}
        (map
          (fn [^java.io.File file]
            [(str/replace (.getName file) #"\.sql$" "")
             (str "EXPLAIN (ANALYZE, FORMAT JSON) "
                  (str/replace (str/trim (slurp file)) #";\s*$" ""))]))
        (->> (.listFiles (io/file "queries"))
             (filter #(and (.isFile ^java.io.File %)
                           (str/ends-with? (.getName ^java.io.File %) ".sql"))))))

(defn- json-number
  [json label]
  (some-> (re-find (re-pattern
                     (str "\"" (java.util.regex.Pattern/quote label)
                          "\": ([0-9.]+)"))
                   json)
          second
          parse-double))

(defn- postgres-query!
  [^Connection conn sql timeout-ms]
  (with-open [statement (.createStatement conn)]
    (.setQueryTimeout statement
                      (int (Math/ceil (/ (double timeout-ms) 1000.0))))
    (with-open [result-set (.executeQuery statement sql)]
      (when-not (.next result-set)
        (throw (ex-info "PostgreSQL EXPLAIN returned no row" {})))
      (let [json         (.getString result-set 1)
            planning-ms (json-number json "Planning Time")
            execution-ms (json-number json "Execution Time")]
        (when-not (and planning-ms execution-ms)
          (throw (ex-info "PostgreSQL EXPLAIN omitted timing"
                          {:json json})))
        {:planning-ms planning-ms
         :execution-ms execution-ms
         :status :ok}))))

(defn- result-row
  [run pass-seed position system entry result]
  [run pass-seed position (name system) (:name entry)
   (when (= :datalevin system) (:sample-seed entry))
   (:planning-ms result) (:execution-ms result) (:result-size result)
   (name (:status result)) (:error result)])

(defn- execute-pass!
  [system run pass-seed query! writer measured? timeout-ms]
  (let [entries (query-entries pass-seed)]
    (doseq [[position entry] (map-indexed vector entries)]
      (print (format "  %s run %d %s ... "
                     (name system) run (:name entry)))
      (flush)
      (let [result
            (try
              (query! entry)
              (catch Throwable e
                {:status (if (timeout-error? e) :timeout :error)
                 :error (str (.getName (class e)) ": " (.getMessage e))}))]
        (println (if (= :ok (:status result))
                   (format "%.3f + %.3f ms"
                           (double (:planning-ms result))
                           (double (:execution-ms result)))
                   (str (:status result) " " (:error result))))
        (when measured?
          (d/write-csv writer
                       [(result-row run pass-seed position
                                    system entry result)])
          (.flush writer))
        (when (and (not measured?) (not= :ok (:status result)))
          (throw (ex-info "External benchmark warm-up failed"
                          {:system system
                           :query (:name entry)
                           :status (:status result)
                           :error (:error result)})))))
    {:queries (count entries)
     :timeout-ms timeout-ms}))

(defn- repository-root
  []
  (loop [directory (.getCanonicalFile (io/file "."))]
    (when directory
      (if (.exists (io/file directory ".git"))
        directory
        (recur (.getParentFile directory))))))

(defn- code-revision
  []
  (when-let [root (repository-root)]
    (let [git-dir (io/file root ".git")
          head    (str/trim (slurp (io/file git-dir "HEAD")))]
      (if (str/starts-with? head "ref: ")
        (let [ref      (subs head 5)
              ref-file (io/file git-dir ref)]
          (if (.exists ref-file)
            (str/trim (slurp ref-file))
            (some
              (fn [line]
                (when (str/ends-with? line (str " " ref))
                  (first (str/split line #"\s+" 2))))
              (when-let [packed (let [file (io/file git-dir "packed-refs")]
                                  (when (.exists file) (slurp file)))]
                (str/split-lines packed)))))
        head))))

(defn- native-library-version
  []
  (some->> (System/getProperty "java.class.path")
           (re-find #"dtlvnative-[^/]+/([^/]+)/dtlvnative")
           second))

(defn- runtime-manifest
  []
  {:code-revision (code-revision)
   :datalevin-version c/version
   :native-library-version (native-library-version)
   :java-version (System/getProperty "java.version")
   :java-vm (System/getProperty "java.vm.name")
   :jvm-options
   (vec (.getInputArguments (ManagementFactory/getRuntimeMXBean)))
   :command (System/getProperty "sun.java.command")
   :os-name (System/getProperty "os.name")
   :os-version (System/getProperty "os.version")
   :os-arch (System/getProperty "os.arch")
   :processors (.availableProcessors (Runtime/getRuntime))})

(defn- postgres-setting
  [^Connection conn setting]
  (with-open [statement (.createStatement conn)
              result-set (.executeQuery statement (str "SHOW " setting))]
    (when (.next result-set)
      (.getString result-set 1))))

(defn- postgres-metadata
  [^Connection conn]
  (let [metadata (.getMetaData conn)
        settings ["server_version" "shared_buffers" "work_mem"
                  "effective_cache_size" "random_page_cost" "seq_page_cost"
                  "max_worker_processes" "max_parallel_workers"
                  "max_parallel_workers_per_gather" "jit" "fsync"
                  "synchronous_commit"]]
    {:product (.getDatabaseProductName metadata)
     :product-version (.getDatabaseProductVersion metadata)
     :driver (.getDriverName metadata)
     :driver-version (.getDriverVersion metadata)
     :settings
     (into (sorted-map)
           (map (fn [setting]
                  [(keyword (str/replace setting "_" "-"))
                   (postgres-setting conn setting)]))
           settings)}))

(defn- database-metadata
  [db-path]
  (let [root  (.getCanonicalFile (io/file db-path))
        files (->> (file-seq root)
                   (filter #(.isFile ^java.io.File %))
                   (remove #(= "lock.mdb" (.getName ^java.io.File %)))
                   (mapv (fn [^java.io.File file]
                           {:path (str (.relativize (.toPath root)
                                                  (.toPath file)))
                            :bytes (.length file)
                            :last-modified (.lastModified file)})))]
    {:path (.getPath root)
     :files files
     :bytes (reduce + (map :bytes files))
     :metadata-fingerprint (hash files)}))

(defn- manifest
  [status config files system-metadata]
  {:status status
   :timestamp (str (Instant/now))
   :config config
   :files files
   :runtime (runtime-manifest)
   :system-metadata system-metadata})

(defn- normalize-system
  [system]
  (keyword (if (keyword? system) (name system) (str system))))

(defn run
  "Run one side of the repeated CIDR external-viability comparison.

  Required: `:system`, either `:datalevin` or `:postgres`. Both invocations
  use the same seed to obtain identical randomized query orders. Datalevin
  additionally derives and records a deterministic sample seed for every
  query. Defaults are one unmeasured warm-up, five measured passes, and a
  60-second per-query timeout."
  [{:keys [system db-path pg-url pg-user pg-pass output-dir seed
           warmup-runs runs query-timeout-ms reject-contaminated?
           require-docker-stopped? reject-pageouts?
           minimum-memory-free-percent]
    :or {db-path "db"
         pg-url "jdbc:postgresql://localhost:5432/postgres"
         pg-user (or (System/getenv "USER") "")
         pg-pass ""
         seed 20261301
         warmup-runs 1
         runs 5
         query-timeout-ms 60000
         reject-contaminated? true
         require-docker-stopped? true
         reject-pageouts? false
         minimum-memory-free-percent 20}}]
  (let [system (normalize-system system)
        _      (when-not (#{:datalevin :postgres} system)
                 (throw (ex-info "System must be :datalevin or :postgres"
                                 {:system system})))
        output-dir (io/file
                     (or output-dir
                         (str "results/cidr-external-" (name system)
                              "-" seed)))
        stamp (System/currentTimeMillis)
        timing-file (io/file output-dir
                             (str "external_timing_" stamp ".csv"))
        health-file (io/file output-dir
                             (str "external_health_" stamp ".edn"))
        manifest-file (io/file output-dir
                               (str "external_manifest_" stamp ".edn"))
        files {:timing-file (.getPath timing-file)
               :health-file (.getPath health-file)
               :manifest-file (.getPath manifest-file)}
        config {:system system
                :db-path (when (= :datalevin system) db-path)
                :pg-url (when (= :postgres system) pg-url)
                :output-dir (.getPath output-dir)
                :seed seed
                :warmup-runs warmup-runs
                :runs runs
                :query-timeout-ms query-timeout-ms
                :queries (query-order seed)
                :warmup-seeds
                (mapv #(+ (long seed) 2000000 (long %))
                      (range warmup-runs))
                :measured-seeds
                (mapv #(+ (long seed) (long %)) (range runs))
                :estimator (when (= :datalevin system) frozen-estimator)
                :fresh-plan-cache-per-query? (= :datalevin system)
                :intermediate-counts? false
                :reject-contaminated? reject-contaminated?
                :require-docker-stopped? require-docker-stopped?
                :reject-pageouts? reject-pageouts?
                :minimum-memory-free-percent minimum-memory-free-percent}
        health-options
        (select-keys config
                     [:reject-contaminated? :require-docker-stopped?
                      :reject-pageouts? :minimum-memory-free-percent])]
    (.mkdirs output-dir)
    (let [resource
          (case system
            :datalevin
            (binding [c/*db-background-sampling?* false]
              (d/get-conn db-path))

            :postgres
            (DriverManager/getConnection pg-url pg-user pg-pass))
          sql-by-name (when (= :postgres system) (sql-files))
          system-metadata
          (case system
            :datalevin (database-metadata db-path)
            :postgres (postgres-metadata resource))
          query!
          (case system
            :datalevin
            #(datalevin-query! (d/db resource) % query-timeout-ms)

            :postgres
            #(postgres-query! resource
                              (or (sql-by-name (:name %))
                                  (throw
                                    (ex-info "Missing PostgreSQL query"
                                             {:query (:name %)})))
                              query-timeout-ms))]
      (spit manifest-file
            (str (pr-str
                   (manifest :running config files system-metadata))
                 "\n"))
      (try
        (with-open [writer (io/writer timing-file)
                    health-writer (io/writer health-file)]
          (d/write-csv writer [timing-header])
          (.flush writer)
          (dotimes [run warmup-runs]
            (let [pass-seed (+ (long seed) 2000000 (long run))]
              (println "External warm-up" (inc run) "of" warmup-runs
                       "for" (name system))
              (evaluation/measured-pass!
                health-writer :warmup run health-options
                #(execute-pass! system run pass-seed query! writer
                                false query-timeout-ms))))
          (dotimes [run runs]
            (let [pass-seed (+ (long seed) (long run))]
              (println "External measured pass" (inc run) "of" runs
                       "for" (name system))
              (evaluation/measured-pass!
                health-writer :measured run health-options
                #(execute-pass! system run pass-seed query! writer
                                true query-timeout-ms)))))
        (spit manifest-file
              (str (pr-str
                     (manifest :complete config files system-metadata))
                   "\n")
              :append true)
        (println "Done:" files)
        files
        (catch Throwable e
          (spit manifest-file
                (str (pr-str
                       (assoc
                         (manifest :failed config files system-metadata)
                         :error
                         (str (.getName (class e)) ": " (.getMessage e))))
                     "\n")
                :append true)
          (throw e))
        (finally
          (case system
            :datalevin (d/close resource)
            :postgres (.close ^Connection resource))
          (u/shutdown-worker-thread-pool))))))
