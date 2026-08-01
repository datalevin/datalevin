(ns datalevin-bench.evaluation
  "Reproducible optimizer ablations for the CIDR evaluation.

  Plain-query timing runs do not enable explain or estimator observers.
  Deterministic plan-only diagnostic replays collect plans and estimator
  observations after timing. Cardinality runs remain separate because counted
  tuple pipes perturb execution time."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [datalevin-bench.cardinality-oracle :as oracle]
   [datalevin-bench.core :as job]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.parser :as dp]
   [datalevin.query :as q]
   [datalevin.query-optimizer :as qo]
   [datalevin.util :as u])
  (:import
   [com.sun.management OperatingSystemMXBean]
   [datalevin.utl LRUCache]
   [java.lang.management ManagementFactory]
   [java.nio.charset StandardCharsets]
   [java.time Instant]
   [java.util ArrayList Base64 Collections Random]
   [java.util.concurrent TimeUnit]))

(def estimator-modes
  "Named experimental conditions. Compatibility aliases are retained for
  scripts written against the first version of the runner."
  {:full
   {:direct-counts? true
    :query-sampling? true
    :estimator-policy :production}

   :no-counts
   {:direct-counts? false
    :query-sampling? true
    :estimator-policy :production}

   :no-sampling
   {:direct-counts? true
    :query-sampling? false
    :estimator-policy :production}

   :raw-sampling
   {:direct-counts? true
    :query-sampling? true
    :estimator-policy :raw}

   :fallback-only
   {:direct-counts? false
    :query-sampling? false
    :estimator-policy :production}

   :shrink-only
   {:direct-counts? true
    :query-sampling? true
    :estimator-policy :shrink}

   :skew-only
   {:direct-counts? true
    :query-sampling? true
    :estimator-policy :skew}

   :shrink-skew
   {:direct-counts? true
    :query-sampling? true
    :estimator-policy :shrink-skew}

   :counts-only
   {:direct-counts? true
    :query-sampling? false
    :estimator-policy :production}

   :sampling-only
   {:direct-counts? false
    :query-sampling? true
    :estimator-policy :production}

   :exact-cardinality
   {:direct-counts? true
    :query-sampling? true
    :estimator-policy :production
    :exact-cardinality? true}})

(def default-modes
  [:full :no-counts :no-sampling :raw-sampling :fallback-only])

(def timing-header
  ["Run" "Seed" "Sample Seed" "Position" "Condition" "Base Mode"
   "Estimator Policy" "Sample Budget" "Prior Size" "Variance Alpha"
   "Tail Pseudo-count" "Conservative Lower Bound" "Baseline"
   "Query Name" "Plan Only" "Worker Rewarmed"
   "Timing Method" "Query Time (ms)"
   "Parsing Time (ms)" "Graph Build Time (ms)" "Optimizer Time (ms)"
   "Preparation Time (ms)" "Execution Time (ms)" "Result Size"
   "Result Hash" "Plan Hash" "Plan Decisions (Base64 EDN)"
   "Plan (Base64 EDN)"
   "Optimized Clauses" "Entity Stars"
   "Search Candidates" "Search States Retained" "Search Pruned"
   "Status" "Error"])

(def cardinality-header
  ["Run" "Seed" "Sample Seed" "Position" "Condition" "Base Mode"
   "Estimator Policy" "Sample Budget" "Prior Size" "Variance Alpha"
   "Tail Pseudo-count" "Conservative Lower Bound" "Baseline"
   "Query Name" "Stage" "Estimated Cardinality" "Actual Cardinality"
   "Q-error" "Estimated Cost" "Steps"])

(def estimator-header
  ["Phase" "Run" "Seed" "Sample Seed" "Position" "Condition" "Base Mode"
   "Estimator Policy" "Sample Budget" "Configured Prior Size"
   "Configured Variance Alpha" "Tail Pseudo-count"
   "Conservative Lower Bound" "Baseline"
   "Query Name" "Observation" "Ratio Key" "Link Type"
   "Attribute" "Index" "Sample Size" "Sample Population"
   "Sample Fingerprint" "Input Size" "Estimated Output"
   "N" "Sum" "Sum Squares" "Maximum" "Mean" "Variance" "CV Squared"
   "Base Ratio" "Prior Size" "Variance Alpha" "Effective Prior"
   "Blended Center" "No-tail Center" "Policy Center"
   "Tail Adjustment" "Lower Bound" "Final Ratio"])

(defn- query-name [query-sym]
  (str/replace (name query-sym) "q-" ""))

(defn- query-value [query-sym]
  (-> (ns-resolve 'datalevin-bench.core query-sym) var-get))

(defonce ^:private exact-cardinality-cache (atom {}))

(defn- exact-cardinality-oracle
  [oracle-dir query-name]
  (let [file (io/file oracle-dir (str query-name ".edn"))
        path (.getCanonicalPath file)
        counts
        (or (get @exact-cardinality-cache path)
            (let [loaded (oracle/read-checkpoint file)]
              (when (empty? loaded)
                (throw (ex-info "Exact-cardinality checkpoint is missing or empty"
                                {:query query-name :file path})))
              (get (swap! exact-cardinality-cache
                          #(if (contains? % path) % (assoc % path loaded)))
                   path)))]
    (fn [{:keys [kind entities] :as request}]
      (when-not (= :subset kind)
        (throw (ex-info "Unsupported exact-cardinality oracle request"
                        {:query query-name :request request})))
      (get counts entities))))

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

(defn- normalize-mode [mode]
  (if (keyword? mode) mode (keyword (str mode))))

(defn- selected-modes [requested]
  (let [modes   (mapv normalize-mode
                      (or (seq requested) default-modes))
        missing (remove estimator-modes modes)]
    (when (seq missing)
      (throw (ex-info "Unknown optimizer evaluation modes"
                      {:unknown (vec missing)
                       :available (vec (sort (keys estimator-modes)))})))
    modes))

(defn- condition-defaults
  [{:keys [sample-size prior-size variance-alpha tail-weight
           conservative-lower-bound?]}]
  {:sample-size sample-size
   :prior-size prior-size
   :variance-alpha variance-alpha
   :tail-weight tail-weight
   :conservative-lower-bound? conservative-lower-bound?})

(defn- nonnegative-finite-number?
  [value]
  (and (number? value)
       (Double/isFinite (double value))
       (not (neg? (double value)))))

(defn- normalize-condition
  [condition defaults]
  (let [condition (if (map? condition)
                    condition
                    {:name (normalize-mode condition)
                     :mode (normalize-mode condition)})
        name-value (or (:name condition) (:mode condition))
        _         (when-not name-value
                    (throw (ex-info "Optimizer condition requires a name"
                                    {:condition condition})))
        name      (normalize-mode name-value)
        mode      (normalize-mode (or (:mode condition) name))
        template  (estimator-modes mode)]
    (when-not template
      (throw (ex-info "Unknown base mode in optimizer condition"
                      {:condition condition
                       :mode mode
                       :available (vec (sort (keys estimator-modes)))})))
    (let [condition (merge template defaults condition
                           {:name name :mode mode})]
      (when-not (and (integer? (:sample-size condition))
                     (pos? (:sample-size condition)))
        (throw (ex-info "Condition sample budget must be a positive integer"
                        {:condition condition})))
      (when-not (nonnegative-finite-number? (:prior-size condition))
        (throw (ex-info "Condition prior size must be non-negative and finite"
                        {:condition condition})))
      (when-not (nonnegative-finite-number? (:variance-alpha condition))
        (throw
          (ex-info
            "Condition variance inflation must be non-negative and finite"
            {:condition condition})))
      (when-not (nonnegative-finite-number? (:tail-weight condition))
        (throw (ex-info "Condition tail pseudo-count must be non-negative"
                        {:condition condition})))
      (when-not (instance? Boolean
                           (:conservative-lower-bound? condition))
        (throw
          (ex-info "Condition conservative lower bound must be boolean"
                   {:condition condition})))
      (when (and (:exact-cardinality? condition)
                 (not (string? (:oracle-dir condition))))
        (throw
          (ex-info "Exact-cardinality condition requires :oracle-dir"
                   {:condition condition})))
      condition)))

(defn- assign-baseline
  [conditions]
  (let [explicit (keep-indexed
                   (fn [i condition]
                     (when (:baseline? condition) i))
                   conditions)]
    (when (< 1 (count explicit))
      (throw (ex-info "Only one optimizer condition may be the baseline"
                      {:conditions conditions})))
    (let [baseline-index
          (or (first explicit)
              (first
                (keep-indexed
                  (fn [i condition]
                    (when (= :full (:name condition)) i))
                  conditions))
              (first
                (keep-indexed
                  (fn [i condition]
                    (when (= :full (:mode condition)) i))
                  conditions)))]
      (mapv (fn [i condition]
              (assoc condition :baseline? (= i baseline-index)))
            (range) conditions))))

(defn- selected-conditions
  [requested modes defaults]
  (when (and (seq requested) (seq modes))
    (throw (ex-info "Specify either :conditions or :modes, not both" {})))
  (let [specs      (or (seq requested)
                       (selected-modes modes))
        conditions (mapv #(normalize-condition % defaults) specs)
        names      (map :name conditions)]
    (when-not (apply distinct? names)
      (throw (ex-info "Optimizer condition names must be unique"
                      {:condition-names (vec names)})))
    (assign-baseline conditions)))

(defn- entry-condition
  [{:keys [condition mode]}]
  (or condition
      (normalize-condition mode
                           {:sample-size c/init-exec-size-threshold
                            :prior-size c/link-estimate-prior-size
                            :variance-alpha c/link-estimate-var-alpha
                            :tail-weight c/link-estimate-tail-weight
                            :conservative-lower-bound?
                            c/link-estimate-conservative-lower-bound?})))

(defn- shuffled [xs seed]
  (let [items (ArrayList. xs)]
    (Collections/shuffle items (Random. (long seed)))
    (vec items)))

(defn- query-sample-seed
  ^long [^long pass-sample-seed ^String query-name]
  (let [query-hash (long (.hashCode query-name))]
    (bit-and 281474976710655
             (bit-xor pass-sample-seed
                      query-hash
                      (bit-shift-left query-hash 32)))))

(defn- schedule [queries conditions run seed pass-sample-seed]
  (-> (for [condition conditions
            query queries]
        {:run         run
         :seed        seed
         :sample-seed (query-sample-seed pass-sample-seed (:name query))
         :mode        (:name condition)
         :condition   condition
         :query       query})
      (shuffled (+ (long seed) (long run)))))

(defn- semantic-result-size [value]
  (cond
    (nil? value) 0
    (counted? value) (count value)
    :else 1))

(defn- result-fingerprint [result]
  (let [value (:result result)]
    [(semantic-result-size value) (hash value)]))

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
  "Record a fingerprint and compare all conditions for a query with the first
  completed condition. This also checks repeated condition runs."
  [observed query-name condition-name fingerprint]
  (let [previous (get-in @observed [query-name :conditions condition-name])
        state    (swap!
                   observed
                   (fn [state]
                     (cond-> (assoc-in
                               state
                               [query-name :conditions condition-name]
                               fingerprint)
                       (nil? (get-in state [query-name :baseline]))
                       (assoc-in [query-name :baseline] fingerprint))))
        baseline (get-in state [query-name :baseline])]
    (and (or (nil? previous) (= previous fingerprint))
         (= baseline fingerprint))))

(defn- query-with-timeout [query-sym timeout-ms]
  (let [query (query-value query-sym)]
    (if timeout-ms
      (assoc (if (map? query) query (dp/query->map query))
             :timeout timeout-ms)
      query)))

(defn- observation-key
  [{:keys [ratio-key link-type attr index sample-size input-size policy
           n sum sumsq max-val]}]
  [ratio-key link-type attr index sample-size input-size policy
   n sum sumsq max-val])

(defn- timeout-error?
  [throwable]
  (loop [e throwable]
    (when e
      (or (= :query/timeout (:type (ex-data e)))
          (re-find #"took too long to run" (or (.getMessage e) ""))
          (recur (.getCause e))))))

(declare plan-records)

(defn- run-query
  [db observed {:keys [mode query sample-seed] :as entry}
   {:keys [intermediate-counts? plan-only? query-timeout-ms timing-method]}]
  (let [{:keys [direct-counts? query-sampling? estimator-policy sample-size
                prior-size variance-alpha tail-weight
                conservative-lower-bound? exact-cardinality? oracle-dir]}
        (entry-condition entry)
        {:keys [name symbol]} query
        observations          (atom {})
        observe!              (fn [observation]
                                (swap! observations assoc
                                       (observation-key observation)
                                       observation))
        collected             #(->> @observations
                                    vals
                                    (sort-by (comp pr-str observation-key))
                                    vec)]
    (try
      (let [{:keys [result fingerprint]}
            (binding [c/use-direct-predicate-counts? direct-counts?
                      c/use-query-local-sampling?   query-sampling?
                      c/link-estimate-policy       estimator-policy
                      c/init-exec-size-threshold   sample-size
                      c/link-estimate-prior-size   prior-size
                      c/link-estimate-var-alpha    variance-alpha
                      c/link-estimate-tail-weight  tail-weight
                      c/link-estimate-conservative-lower-bound?
                      conservative-lower-bound?
                      u/*reservoir-sampling-seed*   sample-seed
                      qo/*cardinality-oracle*
                      (when exact-cardinality?
                        (exact-cardinality-oracle oracle-dir name))
                      qo/*link-estimate-observer*
                      (when (= :explain timing-method) observe!)
                      q/*cache?*                   false
                      q/*plan-cache*               (LRUCache.
                                                     c/query-plan-cache-size)]
              (case timing-method
                :plain-query
                (let [start (System/nanoTime)
                      value (d/q (query-with-timeout symbol query-timeout-ms)
                                 db)
                      elapsed-ms
                      (double (/ (- (System/nanoTime) start) 1000000.0))
                      fingerprint
                      [(semantic-result-size value) (hash value)]]
                  {:result {:timing-method :plain-query
                            :query-time (format "%.3f" elapsed-ms)
                            :actual-result-size (first fingerprint)}
                   :fingerprint fingerprint})

                :explain
                (let [result
                      (d/explain
                        {:run? (not plan-only?)
                         :intermediate-counts? intermediate-counts?}
                        (query-with-timeout symbol query-timeout-ms) db)]
                  {:result result
                   :fingerprint
                   (when-not plan-only? (result-fingerprint result))})

                (throw
                  (ex-info "Unknown timing method"
                           {:timing-method timing-method}))))
            consistent? (or plan-only?
                            (consistent-result?
                              observed name mode fingerprint))]
        {:result                 result
         :fingerprint            fingerprint
         :estimator-observations (collected)
         :status                 (if consistent?
                                   "ok"
                                   "result-mismatch")})
      (catch Throwable e
        {:status                 (if (timeout-error? e) "timeout" "error")
         :estimator-observations (collected)
         :error                  (str (.getName (class e)) ": "
                                      (.getMessage e))}))))

(defn- plan-decisions
  [result]
  (mapv (fn [{:keys [steps size cost recency]}]
          {:steps (mapv str steps)
           :size size
           :cost cost
           :recency recency})
        (plan-records result)))

(defn- canonical-plan-steps
  [steps]
  (mapv
    #(-> %
         (str/replace #"\?bound\d+" "?bound#")
         (str/replace #"\?blank\d+" "?blank#"))
    steps))

(defn- structural-plan-hash
  [decisions]
  (hash (mapv (comp canonical-plan-steps :steps) decisions)))

(defn- base64-text
  [value]
  (.encodeToString (Base64/getEncoder)
                   (.getBytes (str value) StandardCharsets/UTF_8)))

(defn- portable-result
  [result]
  (let [decisions (plan-decisions result)]
    (assoc
      (select-keys result
                   [:parsing-time :building-time :planning-time :prepare-time
                    :execution-time :actual-result-size])
      :plan-edn (pr-str (:plan result))
      :plan-decisions decisions
      :plan-hash (structural-plan-hash decisions)
      :optimized-clause-count (count (:opt-clauses result))
      :entity-star-count (entity-star-count result)
      :search-summary (search-summary result)
      :plan-records
      (mapv #(select-keys % [:size :actual-size :cost :steps])
            (plan-records result)))))

(def ^:private worker-ready-prefix "DATALEVIN_WORKER_READY")
(def ^:private worker-result-prefix "DATALEVIN_WORKER_RESULT ")

(defn- worker-evaluate
  [{:keys [entry options]} db]
  (let [result (run-query db (atom {}) entry options)]
    (cond-> result
      (and (:result result)
           (not= :plain-query (get-in result [:result :timing-method])))
      (update :result portable-result))))

(defn- worker-main
  [db-path]
  (let [conn (binding [c/*db-background-sampling?* false]
               (d/get-conn db-path))]
    (try
      (println worker-ready-prefix)
      (flush)
      (doseq [line (line-seq (io/reader *in*))]
        (let [request (edn/read-string line)
              result  (worker-evaluate request (d/db conn))]
          (println (str worker-result-prefix (pr-str result)))
          (flush)))
      (finally
        (d/close conn)
        (u/shutdown-worker-thread-pool)))))

(defn- worker-java-command
  [db-path]
  [(str (System/getProperty "java.home") "/bin/java")
   "--add-opens=java.base/java.nio=ALL-UNNAMED"
   "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
   "--enable-native-access=ALL-UNNAMED"
   "-cp" (System/getProperty "java.class.path")
   "clojure.main" "-m" "datalevin-bench.evaluation" "worker" db-path])

(defn- read-worker-line
  [reader prefix]
  (loop [messages []]
    (if-let [line (.readLine ^java.io.BufferedReader reader)]
      (if (str/starts-with? line prefix)
        {:line (subs line (count prefix))
         :messages messages}
        (recur (conj messages line)))
      {:messages messages})))

(defn- stop-worker!
  [worker]
  (when worker
    (try (.close ^java.io.Writer (:writer worker))
         (catch Throwable _))
    (let [^Process process (:process worker)]
      (.destroy process)
      (when-not (.waitFor process 2000 TimeUnit/MILLISECONDS)
        (.destroyForcibly process)
        (.waitFor process 2000 TimeUnit/MILLISECONDS)))
    (try (.close ^java.io.Reader (:reader worker))
         (catch Throwable _))))

(defn- start-worker!
  [db-path startup-timeout-ms]
  (let [builder (doto (ProcessBuilder.
                        ^java.util.List (worker-java-command db-path))
                  (.redirectErrorStream true))
        process (.start builder)
        reader  (io/reader (.getInputStream process))
        writer  (io/writer (.getOutputStream process))
        ready   (future (read-worker-line reader worker-ready-prefix))
        result  (deref ready startup-timeout-ms ::timeout)]
    (cond
      (= ::timeout result)
      (do
        (future-cancel ready)
        (stop-worker! {:process process :reader reader :writer writer})
        (throw (ex-info "Optimizer evaluation worker startup timed out"
                        {:timeout-ms startup-timeout-ms})))

      (nil? (:line result))
      (do
        (stop-worker! {:process process :reader reader :writer writer})
        (throw (ex-info "Optimizer evaluation worker exited during startup"
                        {:messages (:messages result)})))

      :else
      {:process process :reader reader :writer writer})))

(defn- ensure-worker!
  [worker-state db-path startup-timeout-ms]
  (or @worker-state
      (let [worker (start-worker! db-path startup-timeout-ms)]
        (reset! worker-state worker)
        worker)))

(defn- stop-current-worker!
  [worker-state worker-needs-rewarm?]
  (stop-worker! @worker-state)
  (reset! worker-state nil)
  (reset! worker-needs-rewarm? true))

(defn- isolated-request
  [entry
   {:keys [worker-state worker-needs-rewarm? db-path query-timeout-ms
           worker-timeout-grace-ms worker-startup-timeout-ms]
    :as options}]
  (let [{:keys [reader writer]}
        (ensure-worker! worker-state db-path worker-startup-timeout-ms)
        request {:entry entry
                 :options
                 (select-keys
                   options
                   [:intermediate-counts? :plan-only? :query-timeout-ms
                    :timing-method
                    :sample-size :prior-size :variance-alpha
                    :tail-weight :conservative-lower-bound?])}
        _       (doto ^java.io.Writer writer
                  (.write (str (pr-str request) "\n"))
                  (.flush))
        response (future (read-worker-line reader worker-result-prefix))
        wait-ms  (+ (long query-timeout-ms)
                    (long worker-timeout-grace-ms))
        result   (deref response wait-ms ::timeout)]
    (if (= ::timeout result)
      (do
        (future-cancel response)
        (stop-current-worker! worker-state worker-needs-rewarm?)
        {:status "timeout"
         :error (str "Worker exceeded wall-clock limit of " wait-ms " ms")
         :estimator-observations []})
      (if-let [payload (:line result)]
        (let [result (edn/read-string payload)]
          (when (= "timeout" (:status result))
            (stop-current-worker! worker-state worker-needs-rewarm?))
          result)
        (do
          (stop-current-worker! worker-state worker-needs-rewarm?)
          {:status "error"
           :error (str "Worker exited without a result: "
                       (str/join "\n" (:messages result)))
           :estimator-observations []})))))

(defn- request-with-rewarm
  [request! entry prewarm-entry worker-needs-rewarm?
   rewarm-restarted-worker? worker-rewarm-runs]
  (let [rewarm?
        (and rewarm-restarted-worker?
             (compare-and-set! worker-needs-rewarm? true false))]
    (if rewarm?
      (let [prewarm-result (request! prewarm-entry)]
        (if (not= "ok" (:status prewarm-result))
          {:status "error"
           :error (str "Worker rewarm query failed with "
                       (:status prewarm-result) ": "
                       (:error prewarm-result))
           :estimator-observations []
           :worker-rewarmed? true}
          (loop [remaining worker-rewarm-runs]
            (if (pos? remaining)
              (let [warm-result (request! entry)]
                (if (= "ok" (:status warm-result))
                  (recur (dec remaining))
                  (assoc warm-result :worker-rewarmed? true)))
              (assoc (request! entry) :worker-rewarmed? true)))))
      (assoc (request! entry) :worker-rewarmed? false))))

(defn- rewarm-entry
  [entry query]
  (assoc entry
         :query query
         :sample-seed
         (query-sample-seed
           (long (:sample-seed entry))
           (:name query))))

(defn- isolated-run-query
  [observed entry
   {:keys [worker-needs-rewarm? rewarm-restarted-worker?
           worker-rewarm-runs worker-rewarm-query]
    :as options}]
  (let [result
        (request-with-rewarm
          #(isolated-request % options)
          entry (rewarm-entry entry worker-rewarm-query)
          worker-needs-rewarm? rewarm-restarted-worker?
          worker-rewarm-runs)
        fingerprint (:fingerprint result)
        consistent? (or (:plan-only? options)
                        (not= "ok" (:status result))
                        (consistent-result?
                          observed
                          (get-in entry [:query :name])
                          (:mode entry)
                          fingerprint))]
    (cond-> result
      (not consistent?) (assoc :status "result-mismatch"))))

(defn- run-query*
  [db observed entry {:keys [isolate-queries?] :as options}]
  (if isolate-queries?
    (isolated-run-query observed entry options)
    (run-query db observed entry options)))

(defn- timing-row
  [{:keys [run seed sample-seed position query result fingerprint
           plan-only? worker-rewarmed? status error] :as entry}]
  (let [explain?  (not= :plain-query (:timing-method result))
        {:keys [candidates retained pruned?]}
        (when explain?
          (or (:search-summary result) (search-summary result)))
        plan?     (or (contains? result :plan-hash)
                      (some? (:plan result)))
        decisions (when plan?
                    (or (:plan-decisions result) (plan-decisions result)))
        {:keys [name mode estimator-policy sample-size prior-size
                variance-alpha tail-weight conservative-lower-bound?
                baseline?]}
        (entry-condition entry)]
    [run seed sample-seed position (clojure.core/name name)
     (clojure.core/name mode) (clojure.core/name estimator-policy) sample-size
     prior-size variance-alpha tail-weight conservative-lower-bound? baseline?
     (:name query) plan-only? (boolean worker-rewarmed?)
     (some-> (:timing-method result) clojure.core/name) (:query-time result)
     (:parsing-time result) (:building-time result) (:planning-time result)
     (:prepare-time result) (:execution-time result) (:actual-result-size result)
     (second fingerprint)
     (when plan? (or (:plan-hash result)
                     (structural-plan-hash decisions)))
     (when plan? (base64-text (pr-str decisions)))
     (when plan?
       (base64-text (or (:plan-edn result) (pr-str (:plan result)))))
     (when explain?
       (or (:optimized-clause-count result) (count (:opt-clauses result))))
     (when explain?
       (or (:entity-star-count result) (entity-star-count result)))
     candidates retained pruned? status error]))

(defn- plan-records [result]
  (or (:plan-records result)
      (filter
        (fn [x]
          (and (map? x)
               (contains? x :steps)
               (contains? x :size)))
        (tree-seq coll? seq (:plan result)))))

(defn- q-error [estimate actual]
  (cond
    (or (nil? estimate) (nil? actual)) nil
    (and (zero? estimate) (zero? actual)) 1.0
    (or (zero? estimate) (zero? actual)) "Inf"
    :else (max (/ (double estimate) (double actual))
               (/ (double actual) (double estimate)))))

(defn- cardinality-rows
  [{:keys [run seed sample-seed position query result] :as entry}]
  (let [{:keys [name mode estimator-policy sample-size prior-size
                variance-alpha tail-weight conservative-lower-bound?
                baseline?]}
        (entry-condition entry)]
    (map-indexed
      (fn [stage {:keys [size actual-size cost steps]}]
        [run seed sample-seed position (clojure.core/name name)
         (clojure.core/name mode) (clojure.core/name estimator-policy)
         sample-size prior-size variance-alpha tail-weight
         conservative-lower-bound? baseline? (:name query) stage
         size actual-size
         (q-error size actual-size) cost
         (str/join " | " steps)])
      (plan-records result))))

(defn- estimator-rows
  [phase {:keys [run seed sample-seed position query
                 estimator-observations] :as entry}]
  (let [{:keys [name mode estimator-policy
                tail-weight conservative-lower-bound? baseline?]
         condition-sample-size :sample-size
         condition-prior-size :prior-size
         condition-variance-alpha :variance-alpha}
        (entry-condition entry)]
    (map-indexed
      (fn [observation
           {:keys [ratio-key link-type attr index sample-size population-size
                   sample-fingerprint input-size estimated-output n sum sumsq
                   max-val mean variance cv2 base-ratio prior-size var-alpha
                   k-eff blended no-tail-center center tail-adjustment
                   lower-bound
                   final-ratio]}]
        [(clojure.core/name phase) run seed sample-seed position
         (clojure.core/name name) (clojure.core/name mode)
         (clojure.core/name estimator-policy) condition-sample-size
         condition-prior-size condition-variance-alpha tail-weight
         conservative-lower-bound? baseline?
         (:name query) observation (pr-str ratio-key)
         (clojure.core/name link-type)
         (pr-str attr) (pr-str index) sample-size population-size
         sample-fingerprint input-size estimated-output n sum sumsq max-val
         mean variance cv2 base-ratio prior-size var-alpha k-eff blended
         no-tail-center center tail-adjustment lower-bound final-ratio])
      estimator-observations)))

(defn- execute-schedule!
  [db observed entries options on-result]
  (doseq [[position entry] (map-indexed vector entries)]
    (let [entry  (assoc entry
                        :position position
                        :plan-only? (:plan-only? options))
          qname  (get-in entry [:query :name])
          mode   (:mode entry)
          _      (print (format "  run %d %s/%s ... "
                                (:run entry) (name mode) qname))
          result (merge entry (run-query* db observed entry options))]
      (println (:status result)
               (when-let [ms (or (get-in result [:result :query-time])
                                 (get-in result [:result :execution-time]))]
                 (str ms " ms")))
      (on-result result))))

(defn- acceptable-warm-up-status?
  [status]
  (contains? #{"ok" "timeout"} status))

(defn- warm-up!
  [db observed queries modes warmup-runs seed options]
  (dotimes [run warmup-runs]
    (println "Warm-up pass" (inc run) "of" warmup-runs)
    (execute-schedule!
      db observed
      (schedule queries modes (- (inc run)) seed
                (+ (long seed) 2000000 (long run)))
      options
      (fn [{:keys [status error query mode]}]
        ;; A timeout is a valid tail outcome, not a harness failure. The
        ;; isolated worker has already been replaced, so continue warming the
        ;; remaining query/condition pairs. Errors and result mismatches still
        ;; reject the pass.
        (when-not (acceptable-warm-up-status? status)
          (throw (ex-info "Optimizer evaluation warm-up failed"
                          {:query (:name query)
                           :mode mode
                           :status status
                           :error error})))))))

(defn- command
  [& args]
  (try
    (let [{:keys [exit out err]} (apply shell/sh args)]
      {:exit exit :out (str/trim out) :err (str/trim err)})
    (catch Throwable e
      {:error (str (.getName (class e)) ": " (.getMessage e))})))

(defn- vm-stat
  []
  (let [{:keys [exit out] :as result} (command "/usr/bin/vm_stat")]
    (if (zero? (long (or exit -1)))
      (let [page-size (some-> (re-find #"page size of (\d+) bytes" out)
                              second
                              parse-long)]
        {:page-size page-size
         :counters
         (into {}
               (keep
                 (fn [[_ label value]]
                   (when-let [n (parse-long (str/replace value "." ""))]
                     [(-> label
                          str/lower-case
                          (str/replace #"[^a-z0-9]+" "-")
                          (str/replace #"(^-|-$)" "")
                          keyword)
                      n])))
               (re-seq #"(?m)^([^:]+):\s+([0-9.]+)\.$" out))})
      result)))

(defn- docker-processes
  []
  (let [{:keys [exit out]} (command "/usr/bin/pgrep" "-ifl"
                                    "Docker\\.app|com\\.docker\\.backend|Docker Desktop")]
    (when (zero? (long (or exit -1)))
      (str/split-lines out))))

(defn- health-snapshot
  [phase pass moment]
  (let [runtime (Runtime/getRuntime)
        os      ^OperatingSystemMXBean
        (ManagementFactory/getOperatingSystemMXBean)]
    {:timestamp              (str (Instant/now))
     :phase                  phase
     :pass                   pass
     :moment                 moment
     :jvm-used-bytes         (- (.totalMemory runtime) (.freeMemory runtime))
     :jvm-total-bytes        (.totalMemory runtime)
     :jvm-max-bytes          (.maxMemory runtime)
     :system-free-bytes      (.getFreeMemorySize os)
     :system-total-bytes     (.getTotalMemorySize os)
     :committed-virtual-bytes (.getCommittedVirtualMemorySize os)
     :system-cpu-load        (.getCpuLoad os)
     :vm-stat                (vm-stat)
     :memory-pressure        (command "/usr/bin/memory_pressure" "-Q")
     :docker-processes       (docker-processes)}))

(defn- vm-counter
  [snapshot counter]
  (get-in snapshot [:vm-stat :counters counter]))

(defn- counter-increased?
  [before after counter]
  (let [a (vm-counter before counter)
        b (vm-counter after counter)]
    (and (number? a) (number? b) (< a b))))

(defn- contamination-reasons
  [before after {:keys [require-docker-stopped? reject-pageouts?
                        minimum-memory-free-percent]}]
  (cond-> []
    (counter-increased? before after :swapouts)
    (conj :swapout)

    (and reject-pageouts?
         (counter-increased? before after :pageouts))
    (conj :pageout)

    (and require-docker-stopped?
         (or (seq (:docker-processes before))
             (seq (:docker-processes after))))
    (conj :docker-running)

    (some
      #(< (long %) (long minimum-memory-free-percent))
      (keep
        (fn [snapshot]
          (some-> (get-in snapshot [:memory-pressure :out])
                  (#(re-find #"free percentage: (\d+)" %))
                  second
                  parse-long))
        [before after]))
    (conj :low-free-memory)))

(defn- counter-delta
  [before after counter]
  (let [a (vm-counter before counter)
        b (vm-counter after counter)]
    (when (and (number? a) (number? b))
      (- b a))))

(defn- write-edn!
  [writer value]
  (binding [*out* writer]
    (prn value))
  (.flush writer))

(defn measured-pass!
  "Run `f` between machine-health snapshots, write both snapshots, and reject
  the interval when the configured Docker, memory, pageout, or swapout gates
  fail. Shared by the optimizer and external-viability experiment runners."
  [health-writer phase pass
   {:keys [reject-contaminated? require-docker-stopped?] :as options} f]
  (let [before (health-snapshot phase pass :before)]
    (write-edn! health-writer before)
    (when (and require-docker-stopped?
               (seq (:docker-processes before)))
      (throw (ex-info "Docker must be stopped for optimizer evaluation"
                      {:phase phase
                       :pass pass
                       :docker-processes (:docker-processes before)})))
    (let [failure (volatile! nil)
          value   (try
                    (f)
                    (catch Throwable e
                      (vreset! failure e)
                      nil))
          after   (health-snapshot phase pass :after)
          reasons (contamination-reasons before after options)]
      (write-edn! health-writer
                  (assoc after
                         :contamination-reasons reasons
                         :vm-delta
                         {:pageouts (counter-delta before after :pageouts)
                          :swapouts (counter-delta before after :swapouts)}
                         :failed? (boolean @failure)))
      (when @failure
        (throw @failure))
      (when (and reject-contaminated? (seq reasons))
        (throw (ex-info "Machine health changed during optimizer evaluation"
                        {:phase phase :pass pass :reasons reasons})))
      value)))

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

(defn- database-manifest
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
  [status config files]
  {:status status
   :timestamp (str (Instant/now))
   :config config
   :files files
   :runtime
   {:datalevin-version c/version
    :code-revision (code-revision)
    :native-library-version (native-library-version)
    :java-version (System/getProperty "java.version")
    :java-vm (System/getProperty "java.vm.name")
    :jvm-options
    (vec (.getInputArguments (ManagementFactory/getRuntimeMXBean)))
    :command (System/getProperty "sun.java.command")
    :os-name (System/getProperty "os.name")
    :os-version (System/getProperty "os.version")
    :os-arch (System/getProperty "os.arch")
    :processors (.availableProcessors (Runtime/getRuntime))}
   :database (database-manifest (:db-path config))
   :estimator
   {:sample-size (:sample-size config)
    :prior-size (:prior-size config)
    :variance-alpha (:variance-alpha config)
    :tail-weight (:tail-weight config)
    :conservative-lower-bound?
    (:conservative-lower-bound? config)
    :conditions (:conditions config)}})

(defn- estimator-config-notes
  [conditions]
  (->> conditions
       (keep
         (fn [{:keys [name estimator-policy tail-weight
                      conservative-lower-bound? exact-cardinality?]}]
           (cond
             exact-cardinality?
             (str name ": exact logical cardinalities replace estimates; "
                  "enumeration, pruning, operators, and the cost model are "
                  "unchanged.")

             (and (= :production estimator-policy)
                  (zero? (double tail-weight))
                  conservative-lower-bound?)
             (str name ": tail pseudo-count is zero; production is max("
                  "raw mean, durable prior, semantic minimum).")

             (and (contains? #{:skew :shrink-skew} estimator-policy)
                  (zero? (double tail-weight)))
             (str name
                  ": tail pseudo-count is zero; the tail term is inactive.")

             :else nil)))
       distinct
       vec))

(defn run
  "Run the CIDR optimizer evaluation.

  Options accepted by `clj -Xeval`:

  `:db-path`             JOB database path, default `\"db\"`
  `:output-dir`          result directory, default `\"results\"`
  `:queries`             vector such as `[\"1a\" \"10c\"]`, default all 113
  `:modes`               named conditions, default the five anchored ablations
  `:conditions`          explicit per-condition maps; supersedes `:modes`
  `:runs`                measured timing/sample passes, default 5
  `:warmup-runs`         unmeasured passes, default 1
  `:cardinality-runs`    counted sample passes, default 5
  `:cardinality-modes`   modes for counted passes, default timing modes
  `:cardinality-conditions` explicit counted conditions
  `:seed`                schedule seed, default 20260724
  `:plan-only?`          plan but do not execute; useful for sample census
  `:timing-method`       `:plain-query` (default) or legacy `:explain`
  `:diagnostic-replay?`  after plain timing, replay each seed plan-only with
                         explain and estimator observation, default true
  `:query-timeout-ms`    cooperative planning/execution timeout, default 30000
  `:isolate-queries?`    run queries in a killable persistent worker, default true
  `:rewarm-restarted-worker?` run fixed and exact unmeasured warmups after a
                        worker restart, default true
  `:worker-rewarm-runs` exact unmeasured replays after restart, default 2
  `:worker-rewarm-query` fixed representative query before exact replays,
                        default `\"6d\"`
  `:reject-contaminated?` fail a pass after health violations, default true
  `:require-docker-stopped?` refuse to start with Docker, default true
  `:minimum-memory-free-percent` reject low-memory passes, default 20
  `:reject-pageouts?`    treat any pageout increase as contamination
  `:sample-size`         query-local reservoir budget, default 1000
  `:prior-size`, `:variance-alpha`, `:tail-weight`, and
  `:conservative-lower-bound?` set and record estimator parameters

  Pass `i` derives one deterministic reservoir-sample seed per query and uses
  it for every selected condition. Thus conditions are paired within a pass, while
  separate passes provide independent deterministic samples. Plain-query timing
  rows are checkpointed without explain instrumentation. Plans and estimator
  observations are checkpointed by a separate deterministic diagnostic replay."
  [{:keys [db-path output-dir queries modes conditions runs warmup-runs
           cardinality-runs cardinality-modes cardinality-conditions
           seed plan-only? timing-method diagnostic-replay? query-timeout-ms
           isolate-queries? rewarm-restarted-worker? worker-rewarm-runs
           worker-rewarm-query
           worker-timeout-grace-ms
           worker-startup-timeout-ms reject-contaminated?
           require-docker-stopped? reject-pageouts?
           minimum-memory-free-percent sample-size prior-size variance-alpha
           tail-weight conservative-lower-bound?]
    :or   {db-path                  "db"
           output-dir               "results"
           runs                     5
           warmup-runs              1
           cardinality-runs         5
           seed                     20260724
           plan-only?               false
           timing-method            :plain-query
           diagnostic-replay?       true
           query-timeout-ms         30000
           isolate-queries?         true
           rewarm-restarted-worker? true
           worker-rewarm-runs       2
           worker-rewarm-query      "6d"
           worker-timeout-grace-ms  5000
           worker-startup-timeout-ms 60000
           reject-contaminated?     true
           require-docker-stopped?  true
           reject-pageouts?         false
           minimum-memory-free-percent 20
           sample-size              c/init-exec-size-threshold
           prior-size               c/link-estimate-prior-size
           variance-alpha           c/link-estimate-var-alpha
           tail-weight              c/link-estimate-tail-weight
           conservative-lower-bound?
           c/link-estimate-conservative-lower-bound?}}]
  (let [_                  (when-not
                             (and (integer? worker-rewarm-runs)
                                  (not (neg? worker-rewarm-runs)))
                             (throw
                               (ex-info
                                 "Worker rewarm runs must be non-negative"
                                 {:worker-rewarm-runs worker-rewarm-runs})))
        _                  (when-not (#{:plain-query :explain} timing-method)
                             (throw
                               (ex-info
                                 "Timing method must be :plain-query or :explain"
                                 {:timing-method timing-method})))
        timing-method      (if plan-only? :explain timing-method)
        diagnostic-replay? (boolean
                             (and diagnostic-replay?
                                  (not plan-only?)
                                  (= :plain-query timing-method)))
        queries            (selected-queries queries)
        worker-rewarm-query-entry
        (first (selected-queries [worker-rewarm-query]))
        defaults           (condition-defaults
                             {:sample-size sample-size
                              :prior-size prior-size
                              :variance-alpha variance-alpha
                              :tail-weight tail-weight
                              :conservative-lower-bound?
                              conservative-lower-bound?})
        conditions         (selected-conditions conditions modes defaults)
        cardinality-conditions
        (cond
          (seq cardinality-conditions)
          (selected-conditions cardinality-conditions nil defaults)

          (seq cardinality-modes)
          (selected-conditions nil cardinality-modes defaults)

          :else conditions)
        formula-notes      (estimator-config-notes
                             (into conditions cardinality-conditions))
        cardinality-runs  (if plan-only? 0 cardinality-runs)
        stamp             (System/currentTimeMillis)
        output-dir        (io/file output-dir)
        timing-file       (io/file output-dir
                                   (str "optimizer_ablation_" stamp ".csv"))
        diagnostic-file   (io/file output-dir
                                   (str "optimizer_diagnostics_" stamp ".csv"))
        cardinality-file  (io/file output-dir
                                   (str "optimizer_cardinality_" stamp ".csv"))
        estimator-file    (io/file output-dir
                                   (str "optimizer_estimates_" stamp ".csv"))
        health-file       (io/file output-dir
                                   (str "optimizer_health_" stamp ".edn"))
        manifest-file     (io/file output-dir
                                   (str "optimizer_manifest_" stamp ".edn"))
        files             {:timing-file (.getPath timing-file)
                           :diagnostic-file (.getPath diagnostic-file)
                           :cardinality-file (.getPath cardinality-file)
                           :estimator-file (.getPath estimator-file)
                           :health-file (.getPath health-file)
                           :manifest-file (.getPath manifest-file)}
        config            {:db-path db-path
                           :output-dir (.getPath output-dir)
                           :queries (mapv :name queries)
                           :conditions conditions
                           :runs runs
                           :warmup-runs warmup-runs
                           :cardinality-runs cardinality-runs
                           :cardinality-conditions cardinality-conditions
                           :seed seed
                           :plan-only? plan-only?
                           :timing-method timing-method
                           :diagnostic-replay? diagnostic-replay?
                           :query-timeout-ms query-timeout-ms
                           :isolate-queries? isolate-queries?
                           :rewarm-restarted-worker?
                           rewarm-restarted-worker?
                           :worker-rewarm-runs worker-rewarm-runs
                           :worker-rewarm-query worker-rewarm-query
                           :worker-timeout-grace-ms worker-timeout-grace-ms
                           :worker-startup-timeout-ms
                           worker-startup-timeout-ms
                           :reject-contaminated? reject-contaminated?
                           :reject-pageouts? reject-pageouts?
                           :minimum-memory-free-percent
                           minimum-memory-free-percent
                           :require-docker-stopped?
                           require-docker-stopped?
                           :sample-size sample-size
                           :prior-size prior-size
                           :variance-alpha variance-alpha
                           :tail-weight tail-weight
                           :conservative-lower-bound?
                           conservative-lower-bound?
                           :estimator-formula-notes formula-notes}
        worker-state       (atom nil)
        worker-needs-rewarm? (atom true)
        run-options       {:plan-only? plan-only?
                           :timing-method timing-method
                           :db-path db-path
                           :query-timeout-ms query-timeout-ms
                           :isolate-queries? isolate-queries?
                           :worker-state worker-state
                           :worker-needs-rewarm? worker-needs-rewarm?
                           :rewarm-restarted-worker?
                           rewarm-restarted-worker?
                           :worker-rewarm-runs worker-rewarm-runs
                           :worker-rewarm-query worker-rewarm-query-entry
                           :worker-timeout-grace-ms worker-timeout-grace-ms
                           :worker-startup-timeout-ms
                           worker-startup-timeout-ms
                           :reject-contaminated? reject-contaminated?
                           :reject-pageouts? reject-pageouts?
                           :minimum-memory-free-percent
                           minimum-memory-free-percent
                           :require-docker-stopped?
                           require-docker-stopped?
                           :sample-size sample-size
                           :prior-size prior-size
                           :variance-alpha variance-alpha
                           :tail-weight tail-weight
                           :conservative-lower-bound?
                           conservative-lower-bound?}
        observed          (atom {})]
    (.mkdirs output-dir)
    (spit manifest-file (str (pr-str (manifest :running config files)) "\n"))
    (println "JOB optimizer evaluation:" (count queries) "queries,"
             (count conditions) "timing conditions")
    (doseq [note formula-notes]
      (println "ESTIMATOR NOTE:" note))
    (let [conn     (when-not isolate-queries?
                     (binding [c/*db-background-sampling?* false]
                       (d/get-conn db-path)))
          database (when conn (d/db conn))]
      (try
        (with-open [timing-writer     (io/writer timing-file)
                    diagnostic-writer (io/writer diagnostic-file)
                    cardinality-writer (io/writer cardinality-file)
                    estimator-writer  (io/writer estimator-file)
                    health-writer     (io/writer health-file)]
          (d/write-csv timing-writer [timing-header])
          (d/write-csv diagnostic-writer [timing-header])
          (d/write-csv cardinality-writer [cardinality-header])
          (d/write-csv estimator-writer [estimator-header])
          (.flush timing-writer)
          (.flush diagnostic-writer)
          (.flush cardinality-writer)
          (.flush estimator-writer)
          (dotimes [run warmup-runs]
            (measured-pass!
              health-writer :warmup run run-options
              #(warm-up! database observed queries conditions 1
                         (+ (long seed) (long run)) run-options)))
          (dotimes [run runs]
            (println (if plan-only?
                       "Plan census pass"
                       "Measured timing pass")
                     (inc run) "of" runs)
            (measured-pass!
              health-writer :timing run run-options
              #(execute-schedule!
                 database observed
                 (schedule queries conditions run seed
                           (+ (long seed) (long run)))
                 (assoc run-options :intermediate-counts? false)
                 (fn [result]
                   (d/write-csv timing-writer [(timing-row result)])
                   (when (seq (:estimator-observations result))
                     (d/write-csv estimator-writer
                                  (estimator-rows :timing result)))
                   (.flush timing-writer)
                   (.flush estimator-writer)))))
          (when diagnostic-replay?
            (dotimes [run runs]
              (println "Diagnostic replay pass" (inc run) "of" runs)
              (measured-pass!
                health-writer :diagnostic run run-options
                #(execute-schedule!
                   database observed
                   (schedule queries conditions run seed
                             (+ (long seed) (long run)))
                   (assoc run-options
                          :timing-method :explain
                          :intermediate-counts? false
                          :plan-only? true)
                   (fn [result]
                     (d/write-csv diagnostic-writer [(timing-row result)])
                     (when (seq (:estimator-observations result))
                       (d/write-csv estimator-writer
                                    (estimator-rows :diagnostic result)))
                     (.flush diagnostic-writer)
                     (.flush estimator-writer))))))
          (dotimes [run cardinality-runs]
            (println "Cardinality pass" (inc run) "of" cardinality-runs)
            (measured-pass!
              health-writer :cardinality run run-options
              #(execute-schedule!
                 database observed
                 (schedule queries cardinality-conditions run seed
                           (+ (long seed) (long run)))
                 (assoc run-options
                        :timing-method :explain
                        :intermediate-counts? true
                        :plan-only? false)
                 (fn [result]
                   (when (= "ok" (:status result))
                     (d/write-csv cardinality-writer
                                  (cardinality-rows result)))
                   (when (seq (:estimator-observations result))
                     (d/write-csv estimator-writer
                                  (estimator-rows :cardinality result)))
                   (.flush cardinality-writer)
                   (.flush estimator-writer))))))
        (let [summary (merge files
                             {:queries (count queries)
                              :timing-conditions
                              (mapv #(select-keys
                                       % [:name :mode :estimator-policy
                                          :sample-size :prior-size
                                          :variance-alpha :tail-weight
                                          :conservative-lower-bound?
                                          :exact-cardinality? :oracle-dir
                                          :baseline?])
                                    conditions)
                              :runs runs
                              :sample-seeds
                              (mapv #(+ (long seed) (long %))
                                    (range runs))
                              :cardinality-conditions
                              (mapv #(select-keys
                                       % [:name :mode :estimator-policy
                                          :sample-size :prior-size
                                          :variance-alpha :tail-weight
                                          :conservative-lower-bound?
                                          :exact-cardinality? :oracle-dir
                                          :baseline?])
                                    cardinality-conditions)
                              :cardinality-runs cardinality-runs
                              :seed seed
                              :plan-only? plan-only?
                              :timing-method timing-method
                              :diagnostic-replay? diagnostic-replay?})]
          (spit manifest-file
                (str (pr-str (manifest :complete config files)) "\n")
                :append true)
          (println "Done:" summary)
          summary)
        (catch Throwable e
          (spit manifest-file
                (str (pr-str
                       (assoc (manifest :failed config files)
                              :error (str (.getName (class e)) ": "
                                          (.getMessage e))))
                     "\n")
                :append true)
          (throw e))
        (finally
          (stop-worker! @worker-state)
          (when conn (d/close conn))
          (u/shutdown-worker-thread-pool))))))

(defn -main [& args]
  (if (= "worker" (first args))
    (worker-main (second args))
    (run {})))
