(ns datalevin-bench.evaluation-validation
  "Acceptance checks for CIDR optimizer-evaluation artifacts."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [datalevin-bench.evaluation-analysis :as analysis]
   [datalevin.core :as d]))

(def ^:private estimator-condition-fields
  #{"Position" "Condition" "Base Mode" "Estimator Policy" "Baseline"})

(defn read-csv-maps
  [path]
  (with-open [reader (io/reader path)]
    (let [[header & rows] (doall (d/read-csv reader))]
      (mapv #(zipmap header %) rows))))

(defn read-edn-forms
  [path]
  (with-open [reader (java.io.PushbackReader. (io/reader path))]
    (loop [forms []]
      (let [form (edn/read {:eof ::eof} reader)]
        (if (= ::eof form)
          forms
          (recur (conj forms form)))))))

(defn- timing-pair-key
  [row]
  [(:run row) (:query row)])

(defn- paired-seed-summary
  [timing]
  (let [conditions (set (map :mode timing))
        groups     (group-by timing-pair-key timing)
        mismatches
        (->> groups
             (keep
               (fn [[key rows]]
                 (let [row-conditions (map :mode rows)
                       sample-seeds   (set (map :sample-seed rows))]
                   (when (or (not= conditions (set row-conditions))
                             (not= (count conditions)
                                   (count row-conditions))
                             (not= 1 (count sample-seeds)))
                     {:key key
                      :conditions (vec (sort row-conditions))
                      :sample-seeds (vec (sort sample-seeds))}))))
             vec)]
    {:groups (count groups)
     :condition-count (count conditions)
     :mismatch-count (count mismatches)
     :mismatches (vec (take 20 mismatches))}))

(defn- sample-source-key
  [row]
  (mapv row ["Phase" "Run" "Sample Seed" "Query Name" "Ratio Key"
             "Link Type" "Attribute" "Index" "Sample Budget" "Sample Size"
             "Sample Population" "Input Size"]))

(defn- sample-fingerprint-summary
  [estimates]
  (let [groups
        (->> estimates
             (filter #(and (contains? #{"timing" "diagnostic"}
                                      (% "Phase"))
                           (seq (% "Sample Fingerprint"))))
             (group-by sample-source-key))
        shared
        (keep
          (fn [[key rows]]
            (let [by-condition (group-by #(% "Condition") rows)]
              (when (< 1 (count by-condition))
                [key by-condition])))
          groups)
        mismatches
        (->> shared
             (keep
               (fn [[key by-condition]]
                 (let [fingerprints
                       (into (sorted-map)
                             (map
                               (fn [[condition rows]]
                                 [condition
                                  (set (map #(% "Sample Fingerprint")
                                            rows))]))
                             by-condition)]
                   (when (not= 1 (count (set (vals fingerprints))))
                     {:key key :fingerprints fingerprints}))))
             vec)]
    {:sample-sources (count groups)
     :shared-sample-sources (count shared)
     :mismatch-count (count mismatches)
     :mismatches (vec (take 20 mismatches))}))

(defn- equivalent-estimator-value
  [row]
  (apply dissoc row estimator-condition-fields))

(defn- equivalent-estimator-key
  [row]
  (mapv row ["Phase" "Run" "Sample Seed" "Query Name"]))

(defn- estimator-equivalence
  [estimates [left right]]
  (let [left  (name left)
        right (name right)
        rows  (filter #(#{left right} (% "Condition")) estimates)
        groups (group-by equivalent-estimator-key rows)
        mismatches
        (->> groups
             (keep
               (fn [[key rows]]
                 (let [by-condition (group-by #(% "Condition") rows)
                       left-values
                       (frequencies
                         (map equivalent-estimator-value
                              (get by-condition left)))
                       right-values
                       (frequencies
                         (map equivalent-estimator-value
                              (get by-condition right)))]
                   (when (not= left-values right-values)
                     {:key key
                      :left-rows (count (get by-condition left))
                      :right-rows (count (get by-condition right))}))))
             vec)]
    {:conditions [(keyword left) (keyword right)]
     :groups (count groups)
     :match-count (- (count groups) (count mismatches))
     :mismatch-count (count mismatches)
     :mismatches (vec (take 20 mismatches))}))

(defn- plan-equivalence
  [timing [left right]]
  (let [condition-set #{left right}
        groups
        (->> timing
             (filter #(condition-set (:mode %)))
             (group-by (juxt :run :sample-seed :query)))
        mismatches
        (->> groups
             (keep
               (fn [[key rows]]
                 (let [by-condition (group-by :mode rows)
                       left-rows    (get by-condition left)
                       right-rows   (get by-condition right)
                       left-plan    (:plan-hash (first left-rows))
                       right-plan   (:plan-hash (first right-rows))]
                   (when (or (not= 1 (count left-rows))
                             (not= 1 (count right-rows))
                             (not= left-plan right-plan))
                     {:key key
                      :left-plan left-plan
                      :right-plan right-plan
                      :left-rows (count left-rows)
                      :right-rows (count right-rows)}))))
             vec)]
    {:conditions [left right]
     :groups (count groups)
     :match-count (- (count groups) (count mismatches))
     :mismatch-count (count mismatches)
     :mismatches (vec (take 20 mismatches))}))

(defn- health-summary
  [health]
  (let [before       (filter #(= :before (:moment %)) health)
        after        (filter #(= :after (:moment %)) health)
        pass-keys    #(set (map (juxt :phase :pass) %))
        contaminated (filter #(seq (:contamination-reasons %)) after)
        failed       (filter :failed? after)
        docker       (filter #(seq (:docker-processes %)) health)
        swapouts     (filter #(pos? (long (or (get-in % [:vm-delta :swapouts])
                                              0)))
                             after)]
    {:records (count health)
     :before-records (count before)
     :after-records (count after)
     :paired-passes? (= (pass-keys before) (pass-keys after))
     :failed-count (count failed)
     :contaminated-count (count contaminated)
     :docker-detected-count (count docker)
     :swapout-pass-count (count swapouts)
     :contamination-reasons
     (frequencies (mapcat :contamination-reasons contaminated))
     :swapout-deltas (mapv #(get-in % [:vm-delta :swapouts]) after)}))

(defn- latest-manifest
  [manifests]
  (last manifests))

(defn- manifest-summary
  [manifests timing]
  (when (seq manifests)
    (let [manifest   (latest-manifest manifests)
          config     (:config manifest)
          expected   (* (count (:queries config))
                        (count (:conditions config))
                        (long (:runs config)))]
      {:records (count manifests)
       :status (:status manifest)
       :expected-timing-rows expected
       :actual-timing-rows (count timing)
       :row-count-matches? (= expected (count timing))})))

(defn validate-artifacts
  [{:keys [timing diagnostics estimates health manifests
           equivalent-condition-pairs]
    :or {equivalent-condition-pairs []}}]
  (let [timing              (if (seq diagnostics)
                              (analysis/merge-diagnostics timing diagnostics)
                              timing)
        status-counts       (frequencies (map :status timing))
        diagnostic-statuses (frequencies (map :status diagnostics))
        diagnostic-valid?
        (or (empty? diagnostics)
            (and (= (count timing) (count diagnostics))
                 (= #{:ok} (set (keys diagnostic-statuses)))
                 (every? :plan-hash diagnostics)))
        invalid-statuses    (select-keys status-counts
                                        [:error :result-mismatch])
        paired-seeds        (paired-seed-summary timing)
        sample-fingerprints (sample-fingerprint-summary estimates)
        health              (health-summary health)
        manifest            (manifest-summary manifests timing)
        equivalence
        (mapv
          (fn [pair]
            {:estimator (estimator-equivalence estimates pair)
             :plan (plan-equivalence timing pair)})
          equivalent-condition-pairs)
        accepted?
        (boolean
          (and (seq timing)
               (seq estimates)
               diagnostic-valid?
               (empty? invalid-statuses)
               (zero? (:mismatch-count paired-seeds))
               (zero? (:mismatch-count sample-fingerprints))
               (pos? (:after-records health))
               (:paired-passes? health)
               (zero? (:failed-count health))
               (zero? (:contaminated-count health))
               (zero? (:docker-detected-count health))
               (zero? (:swapout-pass-count health))
               (or (nil? manifest)
                   (and (= :complete (:status manifest))
                        (:row-count-matches? manifest)))
               (every?
                 (fn [{:keys [estimator plan]}]
                   (and (pos? (:groups estimator))
                        (zero? (:mismatch-count estimator))
                        (pos? (:groups plan))
                        (zero? (:mismatch-count plan))))
                 equivalence)))]
    {:accepted? accepted?
     :timing {:rows (count timing)
              :queries (count (distinct (map :query timing)))
              :conditions (frequencies (map :mode timing))
              :statuses status-counts}
     :diagnostics {:rows (count diagnostics)
                   :statuses diagnostic-statuses
                   :valid? diagnostic-valid?}
     :paired-seeds paired-seeds
     :sample-fingerprints sample-fingerprints
     :health health
     :manifest manifest
     :equivalent-controls equivalence}))

(defn run
  "Validate one optimizer-evaluation artifact set.

  Required: `:timing-file`, `:estimator-file`, and `:health-file`.
  `:diagnostic-file` supplies deterministic plan-only replays for
  uninstrumented timing artifacts. `:manifest-file` is optional.
  `:equivalent-condition-pairs` may name algebraically identical controls, for example
  `[[:production-no-floor :shrink]]`."
  [{:keys [timing-file diagnostic-file estimator-file health-file manifest-file
           equivalent-condition-pairs output-dir]}]
  (doseq [[label path] [[:timing-file timing-file]
                        [:estimator-file estimator-file]
                        [:health-file health-file]]]
    (when-not path
      (throw (ex-info (str "Missing " (name label)) {:option label}))))
  (let [report (validate-artifacts
                 {:timing (analysis/read-timing timing-file)
                  :diagnostics (when diagnostic-file
                                 (analysis/read-timing diagnostic-file))
                  :estimates (read-csv-maps estimator-file)
                  :health (read-edn-forms health-file)
                  :manifests (when manifest-file
                               (read-edn-forms manifest-file))
                  :equivalent-condition-pairs equivalent-condition-pairs})
        stamp (System/currentTimeMillis)
        output-dir (io/file
                     (or output-dir
                         (.getParentFile (io/file timing-file))
                         "."))
        output-file (io/file output-dir
                             (str "optimizer_validation_" stamp ".edn"))]
    (.mkdirs output-dir)
    (spit output-file (str (pr-str report) "\n"))
    (println "Optimizer evaluation validation:"
             {:accepted? (:accepted? report)
              :output-file (.getPath output-file)})
    (when-not (:accepted? report)
      (throw (ex-info "Optimizer evaluation artifacts failed validation"
                      {:output-file (.getPath output-file)
                       :report report})))
    {:accepted? true :output-file (.getPath output-file)}))
