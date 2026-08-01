(ns datalevin-bench.evaluation-analysis
  "Analysis of CIDR optimizer-evaluation timing artifacts."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin.core :as d])
  (:import
   [java.nio.charset StandardCharsets]
   [java.time Instant]
   [java.util Base64 Random]))

(def disagreement-header
  ["Query Name" "Sample Seed" "Conditions" "Plan Hashes"])

(def slowdown-header
  ["Query Name" "Sample Seed" "Condition" "Base Mode" "Estimator Policy"
   "Sample Budget" "Prior Size" "Variance Alpha" "Tail Pseudo-count"
   "Conservative Lower Bound" "Baseline" "Worker Rewarmed" "Status"
   "Timing Method"
   "Planning Time (ms)" "Execution Time (ms)"
   "Charged Execution Time (ms)" "Slowdown" "Catastrophe" "Plan Hash"])

(defn- parse-number [s parser]
  (when (seq s) (parser s)))

(defn- structural-plan-hash
  [encoded-decisions]
  (when (seq encoded-decisions)
    (let [text      (String. (.decode (Base64/getDecoder)
                                      encoded-decisions)
                             StandardCharsets/UTF_8)
          decisions (edn/read-string text)]
      (hash (mapv
              (fn [{:keys [steps]}]
                (mapv
                  #(-> %
                       (str/replace #"\?bound\d+" "?bound#")
                       (str/replace #"\?blank\d+" "?blank#"))
                  steps))
              decisions)))))

(defn- timing-row
  [header values]
  (let [row       (zipmap header values)
        condition (keyword (or (not-empty (row "Condition"))
                               (row "Mode")))
        base-mode (keyword (or (not-empty (row "Base Mode"))
                               (row "Mode")
                               (name condition)))
        baseline? (if (contains? row "Baseline")
                    (= "true" (row "Baseline"))
                    (= :full condition))
        query-ms (parse-number (row "Query Time (ms)") parse-double)
        reported-execution-ms
        (parse-number (row "Execution Time (ms)") parse-double)]
    {:run (parse-number (row "Run") parse-long)
     :seed (parse-number (row "Seed") parse-long)
     :sample-seed (parse-number (row "Sample Seed") parse-long)
     :position (parse-number (row "Position") parse-long)
     ;; Keep :mode as the internal condition key for old analysis callers.
     :mode condition
     :base-mode base-mode
     :policy (keyword (row "Estimator Policy"))
     :sample-budget (parse-number (row "Sample Budget") parse-long)
     :prior-size (parse-number (row "Prior Size") parse-double)
     :variance-alpha (parse-number (row "Variance Alpha") parse-double)
     :tail-weight (parse-number (row "Tail Pseudo-count") parse-double)
     :conservative-lower-bound?
     (when (contains? row "Conservative Lower Bound")
       (= "true" (row "Conservative Lower Bound")))
     :baseline? baseline?
     :query (row "Query Name")
     :plan-only? (= "true" (row "Plan Only"))
     :worker-rewarmed? (= "true" (row "Worker Rewarmed"))
     :timing-method (keyword
                      (or (not-empty (row "Timing Method"))
                          "explain"))
     :planning-ms (parse-number (row "Preparation Time (ms)") parse-double)
     :query-ms query-ms
     :reported-execution-ms reported-execution-ms
     ;; Keep the established analysis key. New artifacts use uninstrumented
     ;; query wall time; legacy explain artifacts use reported execution time.
     :execution-ms (or query-ms reported-execution-ms)
     :result-size (parse-number (row "Result Size") parse-long)
     :result-hash (parse-number (row "Result Hash") parse-long)
     ;; Older artifacts hashed sizes and costs along with the chosen steps.
     ;; Reconstruct a structural hash so estimate changes are not mislabeled
     ;; as changes to the selected plan.
     :plan-hash (or (structural-plan-hash
                      (row "Plan Decisions (Base64 EDN)"))
                    (parse-number (row "Plan Hash") parse-long))
     :status (keyword (row "Status"))
     :error (not-empty (row "Error"))}))

(defn read-timing
  "Read an optimizer timing CSV into typed maps."
  [timing-file]
  (with-open [reader (io/reader timing-file)]
    (let [[header & rows] (doall (d/read-csv reader))]
      (mapv #(timing-row header %) rows))))

(defn- diagnostic-key
  [row]
  [(:run row) (:sample-seed row) (:mode row) (:query row)])

(defn merge-diagnostics
  "Attach plan and planning evidence from deterministic diagnostic replays to
  uninstrumented timing rows. Runtime status and measured time always come
  from the timing artifact."
  [timing diagnostics]
  (let [by-key (group-by diagnostic-key diagnostics)
        duplicates
        (into {}
              (filter (fn [[_ rows]] (not= 1 (count rows))))
              by-key)]
    (when (seq duplicates)
      (throw
        (ex-info "Diagnostic artifact contains duplicate trial keys"
                 {:duplicate-keys (vec (take 20 (keys duplicates)))})))
    (mapv
      (fn [row]
        (if-let [diagnostic (first (get by-key (diagnostic-key row)))]
          (assoc row
                 :planning-ms (:planning-ms diagnostic)
                 :plan-hash (:plan-hash diagnostic)
                 :diagnostic-status (:status diagnostic)
                 :diagnostic-error (:error diagnostic))
          row))
      timing)))

(defn- percentile
  [values p]
  (when (seq values)
    (let [sorted (vec (sort values))
          pos    (* (double p) (double (dec (count sorted))))
          lo     (long (Math/floor pos))
          hi     (long (Math/ceil pos))
          a      (double (nth sorted lo))
          b      (double (nth sorted hi))]
      (+ a (* (- pos lo) (- b a))))))

(defn- distribution
  [values]
  (let [values (vec (filter number? values))]
    {:n (count values)
     :p50 (percentile values 0.50)
     :p90 (percentile values 0.90)
     :p95 (percentile values 0.95)
     :p99 (percentile values 0.99)
     :max (when (seq values) (apply max values))}))

(defn- geometric-mean
  [values]
  (let [values (vec (filter pos? values))]
    (when (seq values)
      (Math/exp (/ (reduce + (map #(Math/log (double %)) values))
                   (double (count values)))))))

(defn- median
  [values]
  (percentile (filter number? values) 0.5))

(defn- baseline-row?
  [row]
  (if (contains? row :baseline?)
    (true? (:baseline? row))
    (= :full (:mode row))))

(defn- baseline-condition
  [rows]
  (let [explicit (vec (distinct
                        (keep #(when (baseline-row? %) (:mode %)) rows)))]
    (when (< 1 (count explicit))
      (throw (ex-info "Timing rows identify multiple baseline conditions"
                      {:baseline-conditions explicit})))
    (first explicit)))

(defn- execution-baselines
  [rows]
  (into {}
        (keep
          (fn [[query query-rows]]
            (when-let [baseline
                       (median
                         (keep #(when (and (baseline-row? %)
                                           (= :ok (:status %)))
                                  (:execution-ms %))
                               query-rows))]
              [query baseline])))
        (group-by :query rows)))

(defn- with-slowdown
  [rows timeout-ms]
  (let [baselines (execution-baselines rows)]
    (mapv
      (fn [{:keys [query status execution-ms] :as row}]
        (let [baseline (baselines query)
              charged  (case status
                         :ok execution-ms
                         :timeout (double timeout-ms)
                         nil)]
          (assoc row
                 :charged-execution-ms charged
                 :slowdown (when (and (number? baseline)
                                      (pos? (double baseline))
                                      (number? charged))
                             (/ (double charged) (double baseline))))))
      rows)))

(defn- modal-count
  [values]
  (when (seq values)
    (apply max (vals (frequencies values)))))

(defn- plan-summary
  [rows]
  (let [by-query (group-by :query (filter :plan-hash rows))
        counts   (into {}
                       (map (fn [[query query-rows]]
                              [query (count (distinct
                                             (map :plan-hash query-rows)))]))
                       by-query)
        total    (reduce + (map count (vals by-query)))
        modal    (reduce + (keep #(modal-count (map :plan-hash %))
                                 (vals by-query)))]
    {:queries (count by-query)
     :distinct-plans-by-query counts
     :flip-count (- total modal)
     :observations total
     :flip-rate (when (pos? total)
                  (/ (double (- total modal)) (double total)))}))

(defn- condition-summary
  [rows]
  (let [statuses      (frequencies (map :status rows))
        runtime-rows  (remove :plan-only? rows)
        slowdowns     (keep :slowdown rows)
        catastrophes  (count (filter #(or (= :timeout (:status %))
                                           (and (number? (:slowdown %))
                                                (<= 10.0
                                                    (double (:slowdown %)))))
                                      runtime-rows))]
    {:base-mode (:base-mode (first rows))
     :estimator-policy (:policy (first rows))
     :sample-budget (:sample-budget (first rows))
     :prior-size (:prior-size (first rows))
     :variance-alpha (:variance-alpha (first rows))
     :tail-weight (:tail-weight (first rows))
     :conservative-lower-bound?
     (:conservative-lower-bound? (first rows))
     :baseline? (baseline-row? (first rows))
     :trials (count rows)
     :runtime-trials (count runtime-rows)
     :completed (get statuses :ok 0)
     :timeouts (get statuses :timeout 0)
     :errors (get statuses :error 0)
     :result-mismatches (get statuses :result-mismatch 0)
     :catastrophes catastrophes
     :planning-ms (distribution (keep :planning-ms
                                      (filter #(= :ok (:status %)) rows)))
     :execution-ms (distribution (keep :execution-ms
                                       (filter #(= :ok (:status %)) rows)))
     :slowdown (distribution slowdowns)
     :geometric-mean-slowdown (geometric-mean slowdowns)
     :capped-execution-ms (reduce + 0.0
                                  (keep :charged-execution-ms rows))
     :plans (plan-summary rows)}))

(defn- catastrophe?
  [row]
  (or (= :timeout (:status row))
      (and (number? (:slowdown row))
           (<= 10.0 (double (:slowdown row))))))

(defn- paired-row-key
  [row]
  [(:run row) (:query row) (:sample-seed row)])

(defn- query-median-slowdowns
  [rows]
  (->> rows
       (group-by :query)
       (keep
         (fn [[query query-rows]]
           (when-let [value (median (keep :slowdown query-rows))]
             {:query query :median-slowdown value})))
       (sort-by (juxt (comp - :median-slowdown) :query))
       vec))

(defn- condition-causal-summary
  [rows baseline condition]
  (let [runtime-rows (remove :plan-only? rows)
        baseline-rows
        (into {}
              (map (juxt paired-row-key identity))
              (filter #(= baseline (:mode %)) rows))
        condition-rows (filter #(= condition (:mode %)) rows)
        condition-runtime-rows
        (filter #(= condition (:mode %)) runtime-rows)
        pairs          (keep
                         (fn [condition-row]
                           (when-let [baseline-row
                                      (baseline-rows
                                        (paired-row-key condition-row))]
                             [baseline-row condition-row]))
                         condition-rows)
        comparable     (filter
                         (fn [[baseline-row condition-row]]
                           (and (= :ok (:status baseline-row))
                                (= :ok (:status condition-row))
                                (:plan-hash baseline-row)
                                (:plan-hash condition-row)))
                         pairs)
        changed        (filter
                         (fn [[baseline-row condition-row]]
                           (not= (:plan-hash baseline-row)
                                 (:plan-hash condition-row)))
                         comparable)
        catastrophes   (filter catastrophe? condition-runtime-rows)
        catastrophe-pairs
        (map
          (fn [condition-row]
            [(baseline-rows (paired-row-key condition-row))
             condition-row])
          catastrophes)
        query-medians  (query-median-slowdowns condition-runtime-rows)
        plan-different-catastrophes
        (count
          (filter
            (fn [[baseline-row condition-row]]
              (and baseline-row
                   (:plan-hash baseline-row)
                   (:plan-hash condition-row)
                   (not= (:plan-hash baseline-row)
                         (:plan-hash condition-row))))
            catastrophe-pairs))
        plan-same-catastrophes
        (count
          (filter
            (fn [[baseline-row condition-row]]
              (and baseline-row
                   (:plan-hash baseline-row)
                   (= (:plan-hash baseline-row)
                      (:plan-hash condition-row))))
            catastrophe-pairs))]
    {:trials (count condition-rows)
     :runtime-trials (count condition-runtime-rows)
     :timeouts (count (filter #(= :timeout (:status %)) condition-rows))
     :query-median-slowdown
     (distribution (map :median-slowdown query-medians))
     :top-query-medians (vec (take 10 query-medians))
     :plan-comparable-pairs (count comparable)
     :plan-changed-pairs (count changed)
     :plan-change-rate (when (seq comparable)
                         (/ (double (count changed))
                            (double (count comparable))))
     :catastrophes (count catastrophes)
     :catastrophe-queries
     (into (sorted-map) (frequencies (map :query catastrophes)))
     :plan-different-catastrophes plan-different-catastrophes
     :plan-same-catastrophes plan-same-catastrophes
     :timeout-catastrophes
     (count (filter #(= :timeout (:status %)) catastrophes))}))

(defn- causal-comparisons
  [rows]
  (let [baseline (baseline-condition rows)]
    (when baseline
      (into (sorted-map)
            (map
              (fn [condition]
                [condition
                 (condition-causal-summary rows baseline condition)]))
            (distinct (map :mode rows))))))

(defn- pairwise-plan-comparison
  [rows left right]
  (let [selected  (filter #(#{left right} (:mode %)) rows)
        groups    (group-by paired-row-key selected)
        pairs     (keep
                    (fn [[_ pair-rows]]
                      (let [by-condition (group-by :mode pair-rows)
                            left-row     (first (by-condition left))
                            right-row    (first (by-condition right))]
                        (when (and (= 1 (count (by-condition left)))
                                   (= 1 (count (by-condition right)))
                                   (= :ok (:status left-row))
                                   (= :ok (:status right-row))
                                   (:plan-hash left-row)
                                   (:plan-hash right-row))
                          [left-row right-row])))
                    groups)
        different (filter
                    (fn [[left-row right-row]]
                      (not= (:plan-hash left-row)
                            (:plan-hash right-row)))
                    pairs)
        pair-counts (frequencies (map (comp :query first) pairs))
        difference-counts
        (frequencies (map (comp :query first) different))
        query-rows
        (->> pair-counts
             (map
               (fn [[query pair-count]]
                 (let [difference-count (get difference-counts query 0)]
                   {:query query
                    :different-pairs difference-count
                    :comparable-pairs pair-count
                    :difference-rate (/ (double difference-count)
                                        (double pair-count))})))
             (sort-by (juxt (comp - :difference-rate)
                            (comp - :different-pairs)
                            :query))
             vec)]
    {:conditions [left right]
     :comparable-pairs (count pairs)
     :different-pairs (count different)
     :difference-rate (when (seq pairs)
                        (/ (double (count different))
                           (double (count pairs))))
     :queries-with-any-difference
     (count (filter #(pos? (:different-pairs %)) query-rows))
     :queries-different-on-every-pair
     (count
       (filter
         #(= (:different-pairs %) (:comparable-pairs %))
         query-rows))
     :top-queries (vec (take 20 query-rows))}))

(defn- pairwise-plan-comparisons
  [rows]
  (let [conditions (vec (sort (distinct (map :mode rows))))]
    (vec
      (for [left-index (range (count conditions))
            right-index (range (inc left-index) (count conditions))]
        (pairwise-plan-comparison
          rows
          (conditions left-index)
          (conditions right-index))))))

(defn- worker-rewarm-summary
  [rows]
  (let [runtime-rows (remove :plan-only? rows)
        rewarmed     (filter :worker-rewarmed? runtime-rows)
        successful  (filter #(= :ok (:status %)) rewarmed)
        successful-by-condition-query
        (group-by (juxt :mode :query)
                  (filter #(= :ok (:status %)) runtime-rows))
        comparisons
        (keep
          (fn [row]
            (let [other-seeds
                  (remove
                    #(= (:sample-seed row) (:sample-seed %))
                    (successful-by-condition-query
                      [(:mode row) (:query row)]))
                  other-seed-median
                  (median (map :execution-ms other-seeds))]
              (when (and (number? other-seed-median)
                         (pos? (double other-seed-median)))
                {:run (:run row)
                 :condition (:mode row)
                 :query (:query row)
                 :execution-ms (:execution-ms row)
                 :other-seed-median-ms other-seed-median
                 :ratio (/ (double (:execution-ms row))
                           (double other-seed-median))})))
          successful)
        sorted-comparisons
        (sort-by (juxt (comp - :ratio) :condition :query) comparisons)]
    {:rows (count rewarmed)
     :statuses (frequencies (map (juxt :mode :status) rewarmed))
     :successful-rows (count successful)
     :successful-comparisons (count comparisons)
     :relative-to-other-seed-median
     (distribution (map :ratio comparisons))
     :over-2x (count (filter #(< 2.0 (:ratio %)) comparisons))
     :top-comparisons (vec (take 10 sorted-comparisons))}))

(defn- condition-bootstrap-metrics
  [rows]
  (let [runtime-rows (remove :plan-only? rows)
        slowdowns    (keep :slowdown runtime-rows)
        trials       (count runtime-rows)]
    {:p95-slowdown (percentile slowdowns 0.95)
     :p99-slowdown (percentile slowdowns 0.99)
     :geometric-mean-slowdown (geometric-mean slowdowns)
     :catastrophe-rate
     (when (pos? trials)
       (/ (double (count (filter catastrophe? rows)))
          (double trials)))}))

(defn- sample-value
  [^Random random values]
  (nth values (.nextInt random (count values))))

(defn- hierarchical-resample
  [rows ^Random random]
  (let [queries (vec (vals (group-by :query rows)))]
    (vec
      (mapcat
        (fn [_]
          (let [query-rows (sample-value random queries)
                seeds      (vec (vals (group-by :sample-seed query-rows)))]
            (mapcat (fn [_] (sample-value random seeds)) seeds)))
        queries))))

(defn- confidence-interval
  [values]
  [(percentile values 0.025) (percentile values 0.975)])

(defn- bootstrap-summary
  [rows samples seed]
  (when (pos? samples)
    (let [random     (Random. (long seed))
          conditions (vec (sort (distinct (map :mode rows))))
          baseline   (baseline-condition rows)
          metrics    [:p95-slowdown :p99-slowdown
                      :geometric-mean-slowdown :catastrophe-rate]
          replicates
          (vec
            (repeatedly
              samples
              (fn []
                (let [sample (hierarchical-resample rows random)]
                  (into {}
                        (map
                          (fn [[condition condition-rows]]
                            [condition
                             (condition-bootstrap-metrics condition-rows)]))
                        (group-by :mode sample))))))
          estimates
          (into {}
                (map (fn [[condition condition-rows]]
                       [condition
                        (condition-bootstrap-metrics condition-rows)]))
                (group-by :mode rows))
          condition-cis
          (into (sorted-map)
                (map
                  (fn [condition]
                    [condition
                     (into {}
                           (map
                             (fn [metric]
                               [metric
                                {:estimate
                                 (get-in estimates [condition metric])
                                 :ci95
                                 (confidence-interval
                                   (keep #(get-in % [condition metric])
                                         replicates))}]))
                           metrics)]))
                conditions)
          contrasts
          (into (sorted-map)
                (for [condition conditions
                      :when (and baseline (not= baseline condition))]
                  [condition
                   (into {}
                         (map
                           (fn [metric]
                             (let [estimate
                                   (when (and
                                           (number?
                                             (get-in estimates
                                                     [condition metric]))
                                           (number?
                                             (get-in estimates
                                                     [baseline metric])))
                                     (- (double
                                          (get-in estimates
                                                  [condition metric]))
                                        (double
                                          (get-in estimates
                                                  [baseline metric]))))
                                   values
                                   (keep
                                     (fn [replicate]
                                       (let [a (get-in replicate
                                                       [condition metric])
                                             b (get-in replicate
                                                       [baseline metric])]
                                         (when (and (number? a) (number? b))
                                           (- (double a) (double b)))))
                                     replicates)]
                               [metric
                               {:estimate estimate
                                 :ci95 (confidence-interval values)}]))
                           metrics))]))
          pairwise-contrasts
          (into (sorted-map)
                (for [left-index (range (count conditions))
                      right-index (range (inc left-index)
                                         (count conditions))
                      :let [left (conditions left-index)
                            right (conditions right-index)]]
                  [[left right]
                   (into {}
                         (map
                           (fn [metric]
                             (let [estimate
                                   (when (and
                                           (number?
                                             (get-in estimates
                                                     [left metric]))
                                           (number?
                                             (get-in estimates
                                                     [right metric])))
                                     (- (double
                                          (get-in estimates [left metric]))
                                        (double
                                          (get-in estimates [right metric]))))
                                   values
                                   (keep
                                     (fn [replicate]
                                       (let [a (get-in replicate
                                                       [left metric])
                                             b (get-in replicate
                                                       [right metric])]
                                         (when (and (number? a) (number? b))
                                           (- (double a) (double b)))))
                                     replicates)]
                               [metric
                                {:estimate estimate
                                 :ci95 (confidence-interval values)}]))
                           metrics))]))]
      {:samples samples
       :seed seed
       :method :hierarchical-paired-query-then-seed-bootstrap
       :baseline-condition baseline
       :conditions condition-cis
       :contrasts-versus-baseline contrasts
       :pairwise-contrasts pairwise-contrasts})))

(defn plan-disagreements
  "Return query/sample pairs where selected conditions chose different plans."
  [rows]
  (->> rows
       (filter #(and (= :ok (:status %)) (:plan-hash %)))
       (group-by (juxt :query :sample-seed))
       (keep
         (fn [[[query sample-seed] pair-rows]]
           (let [plans (into (sorted-map)
                             (map (juxt :mode :plan-hash))
                             pair-rows)]
             (when (< 1 (count (distinct (vals plans))))
               {:query query
                :sample-seed sample-seed
                :plans plans}))))
       (sort-by (juxt :query :sample-seed))
       vec))

(defn summarize
  "Compute the preregistered tail and plan-stability summaries.

  Timeouts are charged at `timeout-ms` and retained in every denominator."
  ([rows timeout-ms]
   (summarize rows timeout-ms {:bootstrap-samples 0}))
  ([rows timeout-ms {:keys [bootstrap-samples bootstrap-seed]
                     :or {bootstrap-samples 0
                          bootstrap-seed 20260727}}]
   (let [rows (with-slowdown rows timeout-ms)]
     {:generated-at (str (Instant/now))
      :timeout-ms timeout-ms
      :trials (count rows)
      :queries (count (distinct (map :query rows)))
      :sample-seeds (count (distinct (map :sample-seed rows)))
      :baseline-condition (baseline-condition rows)
      :conditions (into (sorted-map)
                        (map (fn [[condition condition-rows]]
                               [condition
                                (condition-summary condition-rows)]))
                        (group-by :mode rows))
      :bootstrap (bootstrap-summary
                   rows bootstrap-samples bootstrap-seed)
      :causal-comparisons (causal-comparisons rows)
      :pairwise-plan-comparisons (pairwise-plan-comparisons rows)
      :worker-rewarm (worker-rewarm-summary rows)
      :plan-disagreements (plan-disagreements rows)})))

(defn summarize-query-cohorts
  "Repeat the full analysis for query cohorts selected before execution.

  `query-cohorts` maps cohort names to query-name collections. Unknown query
  names are rejected so a misspelled or stale cohort cannot silently produce
  an incomplete paper result."
  [rows timeout-ms query-cohorts options]
  (let [observed          (set (map :query rows))
        cohort-queries    (into (sorted-map)
                                (map (fn [[cohort queries]]
                                       [cohort (set queries)]))
                                query-cohorts)
        unknown-by-cohort
        (into (sorted-map)
              (keep
                (fn [[cohort queries]]
                  (when-let [unknown (not-empty
                                       (vec (sort
                                              (remove observed queries))))]
                    [cohort unknown])))
              cohort-queries)]
    (when (seq unknown-by-cohort)
      (throw (ex-info "Query cohorts contain unobserved queries"
                      {:unknown-by-cohort unknown-by-cohort})))
    (let [memberships
          (reduce-kv
            (fn [result cohort queries]
              (reduce #(update %1 %2 (fnil conj []) cohort)
                      result queries))
            {}
            cohort-queries)
          assigned (set (keys memberships))]
      {:definitions
       (into (sorted-map)
             (map
               (fn [[cohort queries]]
                 [cohort
                  {:queries (count queries)
                   :trials (count (filter #(contains? queries (:query %))
                                          rows))}]))
             cohort-queries)
       :overlapping-queries
       (into (sorted-map)
             (keep (fn [[query cohorts]]
                     (when (< 1 (count cohorts))
                       [query (vec (sort cohorts))])))
             memberships)
       :unassigned-queries (vec (sort (remove assigned observed)))
       :summaries
       (into (sorted-map)
             (map
               (fn [[cohort queries]]
                 [cohort
                  (summarize
                    (filterv #(contains? queries (:query %)) rows)
                    timeout-ms
                    options)]))
             cohort-queries)})))

(defn- disagreement-row
  [{:keys [query sample-seed plans]}]
  [query sample-seed
   (str/join " | " (map (comp name key) plans))
   (str/join " | " (map (comp str val) plans))])

(defn- slowdown-row
  [{:keys [query sample-seed mode base-mode policy sample-budget prior-size
           variance-alpha tail-weight conservative-lower-bound? baseline?
           worker-rewarmed? status timing-method planning-ms execution-ms
           charged-execution-ms slowdown plan-hash] :as row}]
  [query sample-seed (name mode) (some-> base-mode name) (some-> policy name)
   sample-budget prior-size variance-alpha tail-weight
   conservative-lower-bound? baseline? worker-rewarmed? (name status)
   (name timing-method) planning-ms execution-ms charged-execution-ms slowdown
   (catastrophe? row) plan-hash])

(defn run
  "Analyze one timing CSV.

  Required: `:timing-file`. Optional: `:diagnostic-file` attaches plans and
  planning measurements from deterministic untimed replays. `:timeout-ms`
  defaults to 30000 and `:output-dir` defaults to the timing file's directory.
  `:query-cohorts` may map preregistered cohort names to query-name
  collections."
  [{:keys [timing-file diagnostic-file timeout-ms output-dir
           bootstrap-samples bootstrap-seed query-cohorts]
    :or {timeout-ms 30000
         bootstrap-samples 2000
         bootstrap-seed 20260727}}]
  (when-not timing-file
    (throw (ex-info "Missing :timing-file" {})))
  (let [rows              (cond-> (read-timing timing-file)
                            diagnostic-file
                            (merge-diagnostics
                              (read-timing diagnostic-file)))
        normalized-rows   (with-slowdown rows timeout-ms)
        summary           (summarize rows timeout-ms
                                     {:bootstrap-samples bootstrap-samples
                                      :bootstrap-seed bootstrap-seed})
        summary           (cond-> summary
                            (seq query-cohorts)
                            (assoc
                              :query-cohorts
                              (summarize-query-cohorts
                                rows timeout-ms query-cohorts
                                {:bootstrap-samples bootstrap-samples
                                 :bootstrap-seed bootstrap-seed})))
        stamp             (System/currentTimeMillis)
        output-dir        (io/file
                            (or output-dir
                                (.getParentFile (io/file timing-file))
                                "."))
        summary-file      (io/file output-dir
                                   (str "optimizer_summary_" stamp ".edn"))
        disagreement-file (io/file
                            output-dir
                            (str "optimizer_plan_disagreements_"
                                 stamp ".csv"))
        slowdown-file     (io/file output-dir
                                   (str "optimizer_slowdowns_"
                                        stamp ".csv"))]
    (.mkdirs output-dir)
    (spit summary-file (str (pr-str summary) "\n"))
    (with-open [writer (io/writer disagreement-file)]
      (d/write-csv writer
                   (cons disagreement-header
                         (map disagreement-row
                              (:plan-disagreements summary)))))
    (with-open [writer (io/writer slowdown-file)]
      (d/write-csv writer
                   (cons slowdown-header
                         (map slowdown-row normalized-rows))))
    (let [result {:summary-file (.getPath summary-file)
                  :plan-disagreement-file (.getPath disagreement-file)
                  :slowdown-file (.getPath slowdown-file)
                  :diagnostic-file diagnostic-file
                  :trials (:trials summary)
                  :queries (:queries summary)
                  :sample-seeds (:sample-seeds summary)}]
      (println "Optimizer evaluation analysis:" result)
      result)))
