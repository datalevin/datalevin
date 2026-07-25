(ns datalevin-bench.evaluation
  "Reproducible optimizer ablations for the CIDR evaluation.

  Timing runs never collect intermediate tuple counts. Cardinality runs are
  separate because counted tuple pipes perturb execution time."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin-bench.core :as job]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.query :as q])
  (:import
   [datalevin.utl LRUCache]
   [java.util ArrayList Collections Random]))

(def estimator-modes
  {:full
   {:direct-counts? true
    :query-sampling? true}

   :counts-only
   {:direct-counts? true
    :query-sampling? false}

   :sampling-only
   {:direct-counts? false
    :query-sampling? true}

   :fallback-only
   {:direct-counts? false
    :query-sampling? false}})

(def timing-header
  ["Run" "Seed" "Position" "Mode" "Query Name"
   "Parsing Time (ms)" "Graph Build Time (ms)" "Optimizer Time (ms)"
   "Preparation Time (ms)" "Execution Time (ms)" "Result Size"
   "Result Hash" "Plan Hash" "Optimized Clauses" "Entity Stars"
   "Search Candidates" "Search States Retained" "Search Pruned"
   "Status" "Error"])

(def cardinality-header
  ["Run" "Seed" "Position" "Mode" "Query Name" "Stage"
   "Estimated Cardinality" "Actual Cardinality" "Q-error"
   "Estimated Cost" "Steps"])

(defn- query-name [query-sym]
  (str/replace (name query-sym) "q-" ""))

(defn- query-value [query-sym]
  (-> (ns-resolve 'datalevin-bench.core query-sym) var-get))

(defn- available-queries []
  (into {} (map (juxt query-name identity)) job/queries))

(defn- normalize-query-name [x]
  (if (keyword? x) (name x) (str x)))

(defn- selected-queries [requested]
  (let [available (available-queries)
        names     (if (seq requested)
                    (mapv normalize-query-name requested)
                    (mapv query-name job/queries))
        missing   (remove available names)]
    (when (seq missing)
      (throw (ex-info "Unknown JOB query names"
                      {:unknown (vec missing)
                       :available (vec (sort (keys available)))})))
    (mapv (fn [name] {:name name :symbol (available name)}) names)))

(defn- selected-modes [requested]
  (let [modes   (vec (or (seq requested) (keys estimator-modes)))
        missing (remove estimator-modes modes)]
    (when (seq missing)
      (throw (ex-info "Unknown optimizer evaluation modes"
                      {:unknown (vec missing)
                       :available (vec (keys estimator-modes))})))
    modes))

(defn- shuffled [xs seed]
  (let [items (ArrayList. xs)]
    (Collections/shuffle items (Random. (long seed)))
    (vec items)))

(defn- schedule [queries modes run seed]
  (-> (for [mode modes
            query queries]
        {:run run :mode mode :query query})
      (shuffled (+ (long seed) (long run)))))

(defn- result-fingerprint [result]
  [(:actual-result-size result) (hash (:result result))])

(defn- entity-star-count [result]
  (reduce
    (fn [n nodes]
      (+ n (if (map? nodes) (count nodes) 0)))
    0 (vals (:query-graph result))))

(defn- search-summary [result]
  (reduce
    (fn [{:keys [candidates retained] :as summary}
         {:keys [candidate-count retained-count pruned?]}]
      (assoc summary
             :candidates (+ candidates candidate-count)
             :retained (+ retained retained-count)
             :pruned? (or (:pruned? summary) pruned?)))
    {:candidates 0 :retained 0 :pruned? false}
    (:optimizer-search result)))

(defn- consistent-result?
  "Record a fingerprint and compare all observations with the full optimizer
  when it is available. This also checks repeated full-mode runs."
  [observed query-name mode fingerprint]
  (let [previous (get-in @observed [query-name mode])
        state    (swap! observed assoc-in [query-name mode] fingerprint)
        baseline (get-in state [query-name :full])]
    (and (or (nil? previous) (= previous fingerprint))
         (or (nil? baseline)
             (every? #(= baseline %) (vals (state query-name)))))))

(defn- run-query
  [db observed {:keys [mode query]} intermediate-counts?]
  (let [{:keys [direct-counts? query-sampling?]} (estimator-modes mode)
        {:keys [name symbol]} query]
    (try
      (let [result
            (binding [c/use-direct-predicate-counts? direct-counts?
                      c/use-query-local-sampling?   query-sampling?
                      q/*cache?*                   false
                      q/*plan-cache*               (LRUCache.
                                                     c/query-plan-cache-size)]
              (d/explain {:run? true
                          :intermediate-counts? intermediate-counts?}
                         (query-value symbol) db))
            fingerprint (result-fingerprint result)
            consistent? (consistent-result? observed name mode fingerprint)]
        {:result      result
         :fingerprint fingerprint
         :status      (if consistent? "ok" "result-mismatch")})
      (catch Throwable e
        {:status "error"
         :error  (str (.getName (class e)) ": " (.getMessage e))}))))

(defn- timing-row
  [{:keys [run seed position mode query result fingerprint status error]}]
  (let [{:keys [candidates retained pruned?]} (search-summary result)]
    [run seed position (name mode) (:name query)
     (:parsing-time result) (:building-time result) (:planning-time result)
     (:prepare-time result) (:execution-time result) (:actual-result-size result)
     (second fingerprint) (hash (:plan result)) (count (:opt-clauses result))
     (entity-star-count result) candidates retained pruned? status error]))

(defn- plan-records [result]
  (filter
    (fn [x]
      (and (map? x)
           (contains? x :steps)
           (contains? x :size)))
    (tree-seq coll? seq (:plan result))))

(defn- q-error [estimate actual]
  (cond
    (or (nil? estimate) (nil? actual)) nil
    (and (zero? estimate) (zero? actual)) 1.0
    (or (zero? estimate) (zero? actual)) "Inf"
    :else (max (/ (double estimate) (double actual))
               (/ (double actual) (double estimate)))))

(defn- cardinality-rows
  [{:keys [run seed position mode query result]}]
  (map-indexed
    (fn [stage {:keys [size actual-size cost steps]}]
      [run seed position (name mode) (:name query) stage
       size actual-size (q-error size actual-size) cost
       (str/join " | " steps)])
    (plan-records result)))

(defn- execute-schedule!
  [db observed entries seed intermediate-counts? on-result]
  (doseq [[position entry] (map-indexed vector entries)]
    (let [entry  (assoc entry :seed seed :position position)
          qname  (get-in entry [:query :name])
          mode   (:mode entry)
          _      (print (format "  run %d %s/%s ... "
                                (:run entry) (name mode) qname))
          result (merge entry
                        (run-query db observed entry intermediate-counts?))]
      (println (:status result)
               (when-let [ms (get-in result [:result :execution-time])]
                 (str ms " ms")))
      (on-result result))))

(defn- warm-up!
  [db observed queries modes warmup-runs seed]
  (dotimes [run warmup-runs]
    (println "Warm-up pass" (inc run) "of" warmup-runs)
    (execute-schedule!
      db observed
      (schedule queries modes (- (inc run)) seed)
      seed false
      (fn [{:keys [status error query mode]}]
        (when-not (= "ok" status)
          (throw (ex-info "Optimizer evaluation warm-up failed"
                          {:query (:name query)
                           :mode mode
                           :status status
                           :error error})))))))

(defn run
  "Run the CIDR optimizer evaluation.

  Options accepted by `clj -Xeval`:

  `:db-path`            JOB database path, default `\"db\"`
  `:output-dir`         result directory, default `\"results\"`
  `:queries`            vector such as `[\"1a\" \"10c\"]`, default all 113
  `:modes`              subset of the four estimator modes, default all
  `:runs`               measured timing passes, default 1
  `:warmup-runs`        unmeasured passes, default 1
  `:cardinality-runs`   counted passes, default 1
  `:cardinality-modes`  modes for counted passes, default `[:full]`
  `:seed`               schedule seed, default 20260724

  Each query gets a fresh plan cache. Timing rows use uninstrumented execution;
  estimated/actual cardinalities are written by separate counted passes."
  [{:keys [db-path output-dir queries modes runs warmup-runs cardinality-runs
           cardinality-modes seed]
    :or   {db-path           "db"
           output-dir        "results"
           runs              1
           warmup-runs       1
           cardinality-runs  1
           cardinality-modes [:full]
           seed              20260724}}]
  (let [queries           (selected-queries queries)
        modes             (selected-modes modes)
        cardinality-modes (selected-modes cardinality-modes)
        stamp             (System/currentTimeMillis)
        output-dir        (io/file output-dir)
        timing-file       (io/file output-dir
                                   (str "optimizer_ablation_" stamp ".csv"))
        cardinality-file  (io/file output-dir
                                   (str "optimizer_cardinality_" stamp ".csv"))
        observed          (atom {})
        conn              (d/get-conn db-path)]
    (.mkdirs output-dir)
    (println "JOB optimizer evaluation:" (count queries) "queries,"
             (count modes) "timing modes")
    (try
      (warm-up! (d/db conn) observed queries modes warmup-runs seed)
      (with-open [writer (io/writer timing-file)]
        (d/write-csv writer [timing-header])
        (dotimes [run runs]
          (println "Measured timing pass" (inc run) "of" runs)
          (execute-schedule!
            (d/db conn) observed
            (schedule queries modes run seed)
            seed false
            (fn [result]
              (d/write-csv writer [(timing-row result)])))))
      (with-open [writer (io/writer cardinality-file)]
        (d/write-csv writer [cardinality-header])
        (dotimes [run cardinality-runs]
          (println "Cardinality pass" (inc run) "of" cardinality-runs)
          (execute-schedule!
            (d/db conn) observed
            (schedule queries cardinality-modes run (+ seed 1000000))
            (+ seed 1000000) true
            (fn [result]
              (when (= "ok" (:status result))
                (d/write-csv writer (cardinality-rows result)))))))
      (let [summary {:timing-file      (.getPath timing-file)
                     :cardinality-file (.getPath cardinality-file)
                     :queries          (count queries)
                     :timing-modes     modes
                     :runs             runs
                     :seed             seed}]
        (println "Done:" summary)
        summary)
      (finally
        (d/close conn)))))

(defn -main [& _]
  (run {}))
