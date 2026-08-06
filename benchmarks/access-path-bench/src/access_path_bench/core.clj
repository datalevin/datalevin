(ns access-path-bench.core
  "Compare query execution with physical access paths enabled and disabled."
  (:require
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [clojure.tools.cli :refer [parse-opts]]
   [datalevin.core :as d]
   [datalevin.query :as q]
   [datalevin.query.execute :as qexec]
   [datalevin.util :as u])
  (:import
   [java.time Instant]
   [java.util Random UUID]))

(def ^:private schema
  {:bench/text   {:db/valueType             :db.type/string
                  :db/fulltext              true
                  :db.fulltext/autoDomain   true}
   :bench/vector {:db/valueType :db.type/vec}
   :bench/doc    {:db/valueType :db.type/idoc
                  :db/domain    "bench/doc"}
   :bench/rank   {:db/valueType :db.type/long}
   :bench/keep   {:db/valueType :db.type/boolean}
   :bench/tag    {:db/valueType   :db.type/string
                  :db/cardinality :db.cardinality/many}})

(def ^:private fulltext-query
  '[:find ?e ?score ?tag
    :in $ ?search-opts
    :where
    [(fulltext $ :bench/text
               "needle alpha beta gamma delta epsilon zeta"
               ?search-opts)
     [[?e _ _ ?score]]]
    [?e :bench/keep true]
    [?e :bench/tag ?tag]
    :order-by [?score :desc ?e :asc ?tag :asc]
    :limit 10])

(def ^:private vector-query
  '[:find ?e ?distance ?tag
    :in $ ?query-vector ?search-opts
    :where
    [(vec-neighbors $ :bench/vector ?query-vector ?search-opts)
     [[?e _ _ ?distance]]]
    [?e :bench/keep true]
    [?e :bench/tag ?tag]
    :order-by [?distance :asc ?e :asc ?tag :asc]
    :limit 10])

(def ^:private ave-query
  '[:find ?e ?rank ?tag
    :in $ ?max-rank
    :where
    [?e :bench/rank ?rank]
    [(<= ?rank ?max-rank)]
    [?e :bench/keep true]
    [?e :bench/tag ?tag]
    :order-by [?rank :desc ?e :asc ?tag :asc]
    :limit 10])

(def ^:private idoc-query
  '[:find ?e ?tag
    :in $ ?match
    :where
    [(idoc-match $ :bench/doc ?match) [[?e _ _]]]
    [?e :bench/keep true]
    [?e :bench/tag ?tag]
    :limit 10])

(def ^:private control-query
  '[:find ?e ?tag
    :where
    [?e :bench/keep true]
    [?e :bench/tag ?tag]
    :limit 10])

(def ^:private access-methods qexec/*access-methods*)

(def ^:private cli-options
  [["-n" "--records N" "Synthetic entities to index"
    :default 20000
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   ["-t" "--source-top N" "Fulltext/ANN source-local top-N window"
    :default 10000
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   ["-l" "--limit N" "Root query limit"
    :default 10
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   ["-k" "--keep-every N" "Add metadata tags to every Nth entity"
    :default 10
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   ["-d" "--dimensions N" "Synthetic vector dimensions"
    :default 16
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   [nil "--fanout N" "Metadata tags per qualifying entity"
    :default 64
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   ["-b" "--batch-size N" "Entities per load transaction"
    :default 500
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   ["-w" "--warmup N" "Warmup pairs per workload and mode"
    :default 8
    :parse-fn parse-long
    :validate [(complement neg?) "must not be negative"]]
   ["-i" "--iterations N" "Measured pairs per workload and mode"
    :default 20
    :parse-fn parse-long
    :validate [pos? "must be positive"]]
   ["-s" "--seed N" "Synthetic data seed"
    :default 42
    :parse-fn parse-long]
   [nil "--workloads NAMES"
    "Comma-separated: ave,idoc,fulltext,vector,control"
    :default #{:ave :idoc :fulltext :vector :control}
    :parse-fn #(into #{} (map keyword) (str/split % #","))]
   [nil "--dir PATH" "Database directory; should be empty"]
   [nil "--keep-db" "Keep an automatically-created temporary database"]
   ["-o" "--output PATH" "Write the complete result map as EDN"]
   ["-h" "--help"]])

(defn- query-with-limit
  [query limit]
  (assoc query (dec (count query)) limit))

(defn- db-options
  [dimensions]
  {:wal?          false
   :search-domains {"bench/text" {}}
   :vector-opts   {:dimensions dimensions
                   :metric-type :euclidean}
   :kv-opts       {:wal? false}})

(defn- synthetic-text
  [^Random random]
  (let [terms   ["alpha" "beta" "gamma" "delta" "epsilon" "zeta"]
        padding (.nextInt random 16)]
    (str/join " "
              (concat ["needle"]
                      (mapcat #(repeat (.nextInt random 8) %) terms)
                      (repeat padding "background")))))

(defn- synthetic-vector
  [^Random random dimensions]
  (let [result (float-array dimensions)]
    (dotimes [i dimensions]
      (aset-float result i (- (* 2.0 (.nextFloat random)) 1.0)))
    result))

(defn- synthetic-entity
  [^Random random entity-id
   {:keys [dimensions workloads keep-every tags]}]
  (cond-> {:db/id entity-id}
    (zero? (rem (long entity-id) (long keep-every)))
    (assoc :bench/keep true :bench/tag tags)

    (contains? workloads :fulltext)
    (assoc :bench/text (synthetic-text random))

    (contains? workloads :vector)
    (assoc :bench/vector (synthetic-vector random dimensions))

    (contains? workloads :ave)
    (assoc :bench/rank entity-id)

    (contains? workloads :idoc)
    (assoc :bench/doc {:kind "candidate"
                       :bucket (rem (long entity-id) 17)})))

(defn- load-data!
  [conn {:keys [records batch-size seed] :as opts}]
  (let [random (Random. (long seed))
        tags    (mapv #(str "tag-" %) (range (:fanout opts)))
        opts    (assoc opts :tags tags)
        started (System/nanoTime)]
    (doseq [ids (partition-all batch-size (range 1 (inc records)))]
      (d/transact! conn
                   (mapv #(synthetic-entity random % opts) ids)))
    (/ (- (System/nanoTime) started) 1e9)))

(defn- execute
  [access? query inputs]
  (binding [q/*cache?* false
            qexec/*access-methods* (if access? access-methods [])]
    (apply d/q query inputs)))

(defn- explain
  [query inputs]
  (binding [q/*cache?* false
            qexec/*access-methods* access-methods]
    (apply d/explain {} query inputs)))

(defn- workload
  ([workload-name db opts]
   (workload workload-name db opts nil))
  ([workload-name db
    {:keys [records source-top limit dimensions]} source-counter]
   (let [search-opts
         (when (#{:fulltext :vector} workload-name)
           (cond->
               {:top source-top
                :display (case workload-name
                           :fulltext :refs+scores
                           :vector   :refs+dists)}
             source-counter
             (assoc (case workload-name
                      :fulltext :doc-filter
                      :vector   :vec-filter)
                    (fn [_]
                      (vswap! source-counter inc)
                      true))))
         query       (query-with-limit
                       (case workload-name
                         :ave      ave-query
                         :idoc     idoc-query
                         :fulltext fulltext-query
                         :vector   vector-query
                         :control  control-query)
                       limit)
         query-vector (when (= workload-name :vector)
                        (float-array (repeat dimensions 0.0)))]
     {:name workload-name
      :query query
      :inputs (case workload-name
                :ave      [db records]
                :idoc     [db {:kind "candidate"}]
                :fulltext [db search-opts]
                :vector   [db query-vector search-opts]
                :control  [db])})))

(defn- require-equal-results!
  [workload baseline access]
  (when-not (= baseline access)
    (throw
      (ex-info "Access and conventional query results differ"
               {:workload workload
                :conventional baseline
                :access access}))))

(defn- require-valid-unordered-windows!
  [workload query inputs conventional access]
  (let [complete (set (execute false (query-with-limit query -1) inputs))]
    (when-not (and (every? complete conventional)
                   (every? complete access))
      (throw
        (ex-info "A bounded query returned rows outside its complete result"
                 {:workload workload
                  :conventional conventional
                  :access access})))))

(defn- verify-workload!
  [{:keys [name query inputs]} limit]
  (let [expected-mode (case name
                        :idoc    :adaptive-limit
                        :control nil
                        :adaptive-top-k)
        expect-access? (some? expected-mode)
        plan         (explain query inputs)
        conventional (execute false query inputs)
        access       (execute true query inputs)]
    (when-not (= expect-access? (boolean (:access-path-selected? plan)))
      (throw
        (ex-info "The benchmark query selected an unexpected plan kind"
                 {:workload name
                  :expected-mode expected-mode
                  :explain plan})))
    (when-not (= expected-mode
                 (get-in plan [:selected-plan-alternative :mode]))
      (throw
        (ex-info "The benchmark query selected an unexpected mode"
                 {:workload name
                  :expected expected-mode
                  :mode (get-in plan [:selected-plan-alternative :mode])})))
    (if (= name :idoc)
      (require-valid-unordered-windows!
        name query inputs conventional access)
      (require-equal-results! name conventional access))
    (when-not (= limit (count access))
      (throw
        (ex-info "The benchmark query did not fill its result window"
                 {:workload name :expected limit :actual (count access)})))
    {:plan plan :result access}))

(defn- source-checks
  [workload-name db opts]
  (when (#{:fulltext :vector} workload-name)
    (let [conventional-counter (volatile! 0)
          access-counter       (volatile! 0)
          conventional-workload
          (workload workload-name db opts conventional-counter)
          access-workload
          (workload workload-name db opts access-counter)
          conventional (execute false
                                (:query conventional-workload)
                                (:inputs conventional-workload))
          access       (execute true
                                (:query access-workload)
                                (:inputs access-workload))]
      (require-equal-results! workload-name conventional access)
      {:conventional @conventional-counter
       :access @access-counter})))

(defn- timed-query
  [access? query inputs expected-count]
  (let [started (System/nanoTime)
        result  (execute access? query inputs)
        elapsed (- (System/nanoTime) started)]
    (when-not (= expected-count (count result))
      (throw
        (ex-info "Timed query returned an unexpected row count"
                 {:access? access?
                  :expected expected-count
                  :actual (count result)})))
    elapsed))

(defn- warmup!
  [{:keys [query inputs]} warmup expected-count]
  (dotimes [i warmup]
    (if (even? i)
      (do
        (timed-query true query inputs expected-count)
        (timed-query false query inputs expected-count))
      (do
        (timed-query false query inputs expected-count)
        (timed-query true query inputs expected-count)))))

(defn- measure!
  [{:keys [query inputs]} iterations expected-count]
  (loop [i 0
         conventional []
         access []]
    (if (= i iterations)
      {:conventional conventional :access access}
      (if (even? i)
        (let [conventional-time
              (timed-query false query inputs expected-count)
              access-time
              (timed-query true query inputs expected-count)]
          (recur (inc i)
                 (conj conventional conventional-time)
                 (conj access access-time)))
        (let [access-time
              (timed-query true query inputs expected-count)
              conventional-time
              (timed-query false query inputs expected-count)]
          (recur (inc i)
                 (conj conventional conventional-time)
                 (conj access access-time)))))))

(defn- percentile
  [sorted-values fraction]
  (let [n     (count sorted-values)
        index (min (dec n)
                   (dec (long (Math/ceil (* fraction n)))))]
    (nth sorted-values (max 0 index))))

(defn- latency-stats
  [nanoseconds]
  (let [values (vec (sort nanoseconds))
        in-ms  #(/ (double %) 1e6)]
    {:min       (in-ms (first values))
     :median    (in-ms (percentile values 0.5))
     :p95       (in-ms (percentile values 0.95))
     :max       (in-ms (peek values))
     :mean      (in-ms (/ (reduce + values) (double (count values))))
     :samples-ms (mapv in-ms nanoseconds)}))

(defn- ratio
  [numerator denominator]
  (if (zero? (double denominator))
    ##Inf
    (/ (double numerator) (double denominator))))

(defn- run-workload!
  [db workload-name
   {:keys [records limit warmup iterations source-top] :as opts}]
  (println)
  (println "Benchmarking" (name workload-name) "...")
  (let [workload      (workload workload-name db opts)
        {:keys [plan result]} (verify-workload! workload limit)
        _             (warmup! workload warmup (count result))
        source-checks (source-checks workload-name db opts)
        measurements  (measure! workload iterations (count result))
        conventional  (latency-stats (:conventional measurements))
        access        (latency-stats (:access measurements))
        speedup       (ratio (:median conventional) (:median access))
        candidate-budget
        (get-in plan [:preferred-access-plan :candidate-budget])
        source-window (cond
                        (#{:fulltext :vector} workload-name) source-top
                        (#{:ave :idoc} workload-name)         records
                        :else                                 nil)
        candidate-reduction
        (when (and source-window candidate-budget)
          (ratio source-window candidate-budget))
        result-map
        {:workload workload-name
         :plan-mode (get-in plan [:selected-plan-alternative :mode])
         :candidate-budget candidate-budget
         :source-window source-window
         :result-count (count result)
         :source-checks source-checks
         :planned-candidate-reduction candidate-reduction
         :latency-ms {:conventional conventional :access access}
         :median-speedup speedup}]
    (printf "  conventional median: %.3f ms (p95 %.3f)%n"
            (double (:median conventional)) (double (:p95 conventional)))
    (printf "  access median:       %.3f ms (p95 %.3f)%n"
            (double (:median access)) (double (:p95 access)))
    (printf "  median speedup:      %.2fx%n" (double speedup))
    (when candidate-reduction
      (printf "  candidate window:    %,d -> <=%,d (%.2fx smaller)%n"
              (long source-window) (long candidate-budget)
              (double candidate-reduction)))
    (when source-checks
      (printf "  source checks:       %,d -> %,d%n"
              (long (:conventional source-checks))
              (long (:access source-checks))))
    result-map))

(defn- host-info
  []
  {:timestamp  (str (Instant/now))
   :java       (System/getProperty "java.version")
   :vm         (System/getProperty "java.vm.name")
   :os         (System/getProperty "os.name")
   :os-version (System/getProperty "os.version")
   :arch       (System/getProperty "os.arch")
   :processors (.availableProcessors (Runtime/getRuntime))})

(defn- validate-options
  [{:keys [records source-top limit keep-every fanout workloads] :as opts}]
  (let [known #{:ave :idoc :fulltext :vector :control}
        unknown (seq (remove known workloads))]
    (cond
      (> source-top records)
      "--source-top must not exceed --records"

      (> limit (* fanout (quot records keep-every)))
      "--records and --keep-every cannot produce enough qualifying rows"

      (empty? workloads)
      "--workloads must not be empty"

      unknown
      (str "unknown workloads: " (str/join "," (map name unknown)))

      :else opts)))

(defn run-benchmark
  [opts]
  (let [auto-dir? (nil? (:dir opts))
        dir       (or (:dir opts)
                      (str (u/tmp-dir
                             (str "access-path-bench-" (UUID/randomUUID)))))
        db-opts   (db-options (:dimensions opts))
        started   (System/nanoTime)]
    (println "Access-path benchmark")
    (println "Database:" dir)
    (println "Configuration:"
             (select-keys opts
                          [:records :source-top :limit :keep-every
                           :dimensions :fanout :warmup :iterations
                           :workloads]))
    (try
      (let [loader (d/create-conn dir schema db-opts)
            load-seconds
            (try
              (load-data! loader opts)
              (finally
                ;; Reopening separates query measurements from vector
                ;; checkpoint work initiated during loading.
                (d/close loader)))
            conn (d/get-conn dir schema db-opts)]
        (try
          (println "Loaded and indexed in"
                   (format "%.2f seconds" (double load-seconds)))
          (let [db      (d/db conn)
                results (mapv #(run-workload! db % opts)
                              (sort-by name (:workloads opts)))
                report  {:host (host-info)
                         :configuration
                         (select-keys opts
                                      [:records :source-top :limit :keep-every
                                       :dimensions :fanout :batch-size :warmup
                                       :iterations :seed :workloads])
                         :load-seconds load-seconds
                         :elapsed-seconds
                         (/ (- (System/nanoTime) started) 1e9)
                         :results results}]
            (when-let [output (:output opts)]
              (spit output (with-out-str (pprint/pprint report)))
              (println)
              (println "Wrote results to" output))
            report)
          (finally
            (d/close conn))))
      (finally
        (when (and auto-dir? (not (:keep-db opts)))
          (u/delete-files dir))))))

(defn- usage
  [summary]
  (str "Access-path enabled-vs-disabled benchmark\n\n"
       "Usage: clojure -M:bench [options]\n\n"
       "Options:\n" summary))

(defn -main
  [& args]
  (let [{:keys [options errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options)
      (println (usage summary))

      (seq errors)
      (do
        (binding [*out* *err*]
          (doseq [error errors] (println error))
          (println)
          (println (usage summary)))
        (System/exit 1))

      :else
      (if-let [validated (validate-options options)]
        (if (string? validated)
          (do
            (binding [*out* *err*] (println validated))
            (System/exit 1))
          (run-benchmark validated))
        (System/exit 1)))))
