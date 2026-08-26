(ns ldbc-snb-bench.comparison
  "Validate and compare two reports produced by the shared LDBC harness."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as str])
  (:import
   [java.io File]
   [java.util Locale]))

(def ^:private comparable-configuration-keys
  [:warmup :iterations :parameter-count :seed :scale-factor :verify?
   :query-cache? :query-names :run-role])

(def ^:private comparable-host-keys
  [:processors :java :vm :os :os-version :arch :max-heap-bytes
   :clojure-direct-linking])

(defn- fail!
  [message data]
  (throw (ex-info message data)))

(defn- require-equal!
  [label left right]
  (when-not (= left right)
    (fail! (str "Reports are not comparable: " label " differs")
           {:field label :left left :right right})))

(defn- successful-report!
  [side report]
  (when-not (zero? (:exit-code report))
    (fail! (str side " report did not complete successfully")
           {:side side :exit-code (:exit-code report)}))
  (when-let [failed (seq (remove #(= :ok (:status %)) (:summaries report)))]
    (fail! (str side " report contains failed query summaries")
           {:side side
            :failed (mapv #(select-keys % [:query :status]) failed)})))

(defn- summary-index
  [report]
  (into {} (map (juxt :query identity)) (:summaries report)))

(defn- result-key
  [result]
  [(:name result) (:selection-index result) (:source-index result)])

(defn- result-index
  [report]
  (into {} (map (juxt result-key identity)) (:results report)))

(defn- process-identity
  [report]
  (select-keys (:host report) [:process-id :jvm-instance-id]))

(defn- independent-processes!
  [warmup measurement]
  (let [warmup-process (process-identity warmup)
        measurement-process (process-identity measurement)
        warmup-instance (:jvm-instance-id warmup-process)
        measurement-instance (:jvm-instance-id measurement-process)]
    (when-not (and (string? warmup-instance)
                   (string? measurement-instance))
      (fail! "Reports do not identify their JVM instances"
             {:warmup-process warmup-process
              :measurement-process measurement-process}))
    (when (= warmup-instance measurement-instance)
      (fail! "Warmup and measurement reports came from the same JVM process"
             {:process warmup-process}))
    {:warmup warmup-process
     :measurement measurement-process}))

(defn verify-independent-pass
  "Verify that independent warmup and measurement processes produced the same
   schedule and exact canonical result digest for every query/parameter pair."
  ([warmup measurement]
   (verify-independent-pass warmup measurement {}))
  ([warmup measurement {:keys [warmup-path measurement-path]}]
   (successful-report! "Warmup" warmup)
   (successful-report! "Measurement" measurement)
   (require-equal! :benchmark-system
                   (:benchmark-system warmup)
                   (:benchmark-system measurement))
   (when-not (= :warmup (get-in warmup [:configuration :run-role]))
     (fail! "Warmup report is not labeled with run role warmup"
            {:run-role (get-in warmup [:configuration :run-role])}))
   (when-not (= :measurement (get-in measurement
                                     [:configuration :run-role]))
     (fail! "Measurement report is not labeled with run role measurement"
            {:run-role (get-in measurement [:configuration :run-role])}))
   (require-equal! :benchmark-suite
                   (:benchmark-suite warmup) (:benchmark-suite measurement))
   (require-equal! :configuration
                   (dissoc (select-keys (:configuration warmup)
                                        comparable-configuration-keys)
                           :run-role)
                   (dissoc (select-keys (:configuration measurement)
                                        comparable-configuration-keys)
                           :run-role))
   (require-equal! :timing-boundary
                   (:timing-boundary warmup) (:timing-boundary measurement))
   (require-equal! :parameter-schedule-sha256
                   (get-in warmup [:parameter-schedule :sha256])
                   (get-in measurement [:parameter-schedule :sha256]))
   (require-equal! :host
                   (select-keys (:host warmup) comparable-host-keys)
                   (select-keys (:host measurement) comparable-host-keys))
   (require-equal! :query-implementation
                   (get-in warmup [:neo4j :cypher-sha256])
                   (get-in measurement [:neo4j :cypher-sha256]))
   (require-equal! :schema-definition
                   (get-in warmup [:neo4j :schema-sha256])
                   (get-in measurement [:neo4j :schema-sha256]))
   (require-equal! :database-indexes
                   (get-in warmup [:neo4j :server :indexes])
                   (get-in measurement [:neo4j :server :indexes]))
   (let [processes (independent-processes! warmup measurement)
         warmup-results (result-index warmup)
         measurement-results (result-index measurement)]
     (require-equal! :result-keys
                     (set (keys warmup-results))
                     (set (keys measurement-results)))
     (let [mismatches
           (->> (keys warmup-results)
                (filter
                  (fn [key]
                    (let [warmup-result (warmup-results key)
                          measurement-result (measurement-results key)]
                      (or (not= (:result-count warmup-result)
                                (:result-count measurement-result))
                          (not= (:result-sha256 warmup-result)
                                (:result-sha256 measurement-result))))))
                sort
                vec)]
       (when (seq mismatches)
         (fail! "Independent warmup and measurement results differ"
                {:system (:benchmark-system measurement)
                 :mismatches mismatches}))
       {:system (:benchmark-system measurement)
        :warmup-report-path warmup-path
        :measurement-report-path measurement-path
        :parameter-schedule-sha256
        (get-in measurement [:parameter-schedule :sha256])
        :result-count (count measurement-results)
        :independent-client-processes? true
        :processes processes
        :exact-result-digests-match? true}))))

(defn- validate-comparable!
  [left right]
  (successful-report! "Left" left)
  (successful-report! "Right" right)
  (require-equal! :benchmark-suite
                  (:benchmark-suite left) (:benchmark-suite right))
  (require-equal! :format-version
                  (:format-version left) (:format-version right))
  (require-equal! :official-ldbc-result
                  (:official-ldbc-result left)
                  (:official-ldbc-result right))
  (require-equal! :configuration
                  (select-keys (:configuration left)
                               comparable-configuration-keys)
                  (select-keys (:configuration right)
                               comparable-configuration-keys))
  (require-equal! :timing-boundary
                  (:timing-boundary left) (:timing-boundary right))
  (require-equal! :scale-factor
                  (get-in left [:dataset :scale-factor])
                  (get-in right [:dataset :scale-factor]))
  (require-equal! :parameter-schedule-sha256
                  (get-in left [:parameter-schedule :sha256])
                  (get-in right [:parameter-schedule :sha256]))
  (require-equal! :host
                  (select-keys (:host left) comparable-host-keys)
                  (select-keys (:host right) comparable-host-keys))
  (require-equal! :queries
                  (mapv :query (:summaries left))
                  (mapv :query (:summaries right)))
  (require-equal! :result-keys
                  (set (keys (result-index left)))
                  (set (keys (result-index right)))))

(defn- positive-latency!
  [system query latency]
  (when-not (and (number? latency) (pos? latency))
    (fail! "Report contains a missing or non-positive median latency"
           {:system system :query query :median-ms latency})))

(defn- comparison-row
  [left-name right-name left-summary right-summary digest-match?]
  (let [query (:query left-summary)
        left-median (get-in left-summary [:latency-ms :median])
        right-median (get-in right-summary [:latency-ms :median])]
    (positive-latency! left-name query left-median)
    (positive-latency! right-name query right-median)
    (require-equal! (str query " sample count")
                    (:sample-count left-summary)
                    (:sample-count right-summary))
    (require-equal! (str query " result counts")
                    (:result-counts left-summary)
                    (:result-counts right-summary))
    (let [ratio (/ (double right-median) (double left-median))]
      {:query query
       :result-counts (:result-counts left-summary)
       :sample-count (:sample-count left-summary)
       :left-median-ms left-median
       :left-p95-ms (get-in left-summary [:latency-ms :p95])
       :right-median-ms right-median
       :right-p95-ms (get-in right-summary [:latency-ms :p95])
       :right-over-left-median-ratio ratio
       :lower-median-system (if (< ratio 1.0) right-name left-name)
       :lower-median-factor (if (< ratio 1.0) (/ 1.0 ratio) ratio)
       :exact-result-digest-match? digest-match?})))

(defn- geometric-mean
  [values]
  (when (seq values)
    (Math/exp (/ (reduce + (map #(Math/log (double %)) values))
                 (double (count values))))))

(defn- aggregate
  [rows]
  (when (seq rows)
    (let [left-sum (reduce + (map :left-median-ms rows))
          right-sum (reduce + (map :right-median-ms rows))
          ratios (map :right-over-left-median-ratio rows)]
      {:query-count (count rows)
       :left-sum-of-medians-ms left-sum
       :right-sum-of-medians-ms right-sum
       :right-over-left-sum-ratio (/ right-sum left-sum)
       :geometric-mean-right-over-left-median-ratio (geometric-mean ratios)
       :left-lower-median-count
       (count (filter #(> (:right-over-left-median-ratio %) 1.0) rows))
       :right-lower-median-count
       (count (filter #(< (:right-over-left-median-ratio %) 1.0) rows))})))

(defn compare-reports
  "Validate comparable harness reports and return per-query/aggregate metrics."
  ([left right]
   (compare-reports left right {}))
  ([left right {:keys [left-path right-path]}]
   (validate-comparable! left right)
   (let [left-name (:benchmark-system left)
         right-name (:benchmark-system right)
         left-summaries (summary-index left)
         right-summaries (summary-index right)
         left-results (result-index left)
         right-results (result-index right)
         digest-matches
         (into {}
               (map (fn [query]
                      (let [keys-for-query
                            (filter #(= query (first %))
                                    (keys left-results))]
                        [query
                         (every?
                           #(= (:result-sha256 (left-results %))
                               (:result-sha256 (right-results %)))
                           keys-for-query)])))
               (map :query (:summaries left)))
         rows (mapv (fn [{:keys [query]}]
                      (comparison-row
                        left-name right-name
                        (left-summaries query)
                        (right-summaries query)
                        (digest-matches query)))
                    (:summaries left))
         ic-rows (filterv #(str/starts-with? (:query %) "IC") rows)
         is-rows (filterv #(str/starts-with? (:query %) "IS") rows)]
     {:format-version 1
      :benchmark-suite (:benchmark-suite left)
      :official-ldbc-result false
      :systems
      {:left {:name left-name
              :report-path left-path
              :host (:host left)
              :dataset (:dataset left)}
       :right {:name right-name
               :report-path right-path
               :host (:host right)
               :dataset (:dataset right)}}
      :methodology
      {:configuration (select-keys (:configuration left)
                                   comparable-configuration-keys)
       :timing-boundary (:timing-boundary left)
       :parameter-schedule-sha256
       (get-in left [:parameter-schedule :sha256])}
      :result-validation
      {:result-counts-match? true
       :exact-result-digest-match-count (count (filter val digest-matches))
       :query-count (count digest-matches)
       :exact-result-digest-matches
       (mapv :query (filter :exact-result-digest-match? rows))
       :different-result-representations
       (mapv :query (remove :exact-result-digest-match? rows))}
      :aggregates
      {:all (aggregate rows)
       :interactive-complex (aggregate ic-rows)
       :interactive-short (aggregate is-rows)}
      :rows rows})))

(defn- fixed
  [value]
  (String/format Locale/ROOT "%,.3f" (object-array [(double value)])))

(defn- faster-label
  [{:keys [lower-median-system lower-median-factor]}]
  (str lower-median-system " " (fixed lower-median-factor) "x"))

(defn markdown
  "Render a compact Markdown report."
  [comparison]
  (let [left-name (get-in comparison [:systems :left :name])
        right-name (get-in comparison [:systems :right :name])
        rows (:rows comparison)
        aggregates (:aggregates comparison)
        one-sample? (every? #(= 1 (:sample-count %)) rows)
        latency-label (if one-sample? "measured" "median")
        sum-label (if one-sample? "sum of measured times" "sum of medians")]
    (str
      "| Query | " left-name " " latency-label " (ms) | " right-name
      " " latency-label " (ms) | " right-name " / " left-name " | Lower time |\n"
      "|---|---:|---:|---:|---|\n"
      (apply str
             (map (fn [{:keys [query left-median-ms right-median-ms
                               right-over-left-median-ratio]
                        :as row}]
                    (str "| " query " | " (fixed left-median-ms)
                         " | " (fixed right-median-ms)
                         " | " (fixed right-over-left-median-ratio)
                         "x | " (faster-label row) " |\n"))
                  rows))
      "\n"
      "| Query set | " left-name " " sum-label " (ms) | " right-name
      " " sum-label " (ms) | " right-name " / " left-name
      " sum | Geomean per-query ratio |\n"
      "|---|---:|---:|---:|---:|\n"
      (apply str
             (keep (fn [[label key]]
                     (when-let [summary (get aggregates key)]
                      (str "| " label
                           " | " (fixed (:left-sum-of-medians-ms summary))
                           " | " (fixed (:right-sum-of-medians-ms summary))
                           " | " (fixed (:right-over-left-sum-ratio summary))
                           "x | "
                           (fixed
                             (:geometric-mean-right-over-left-median-ratio
                               summary))
                           "x |\n")))
                  [["All" :all]
                   ["IC1-IC14" :interactive-complex]
                   ["IS1-IS7" :interactive-short]]))
      (when-let [validation (:independent-pass-validation comparison)]
        (str "\nIndependent-process replay validation: "
             (str/join
               "; "
               (map (fn [side]
                      (let [{:keys [system result-count]}
                            (get validation side)]
                        (str system " " result-count
                             " exact result digests")))
                    [:left :right]))
             ".\n")))))

(defn- ensure-parent!
  [path]
  (when-let [parent (.getParentFile ^File (io/file path))]
    (.mkdirs parent)))

(defn- csv-cell
  [value]
  (let [value (if (nil? value) "" (str value))]
    (if (re-find #"[\",\r\n]" value)
      (str "\"" (str/replace value "\"" "\"\"") "\"")
      value)))

(defn write-csv!
  [path comparison]
  (ensure-parent! path)
  (let [one-sample? (every? #(= 1 (:sample-count %)) (:rows comparison))
        latency-label (if one-sample? "Measured" "Median")]
    (with-open [writer (io/writer path :encoding "UTF-8")]
      (.write writer
              (str "Query,Sample Count,Left System,Right System,Left "
                   latency-label " (ms),Left P95 (ms),Right " latency-label
                   " (ms),Right P95 (ms),Right / Left " latency-label
                   " Ratio,Lower Time System,Lower Time Factor,"
                   "Result Counts,Exact Digest Match\n"))
    (doseq [row (:rows comparison)]
      (.write
        writer
        (str
          (str/join
            ","
            (map csv-cell
                 [(:query row)
                  (:sample-count row)
                  (get-in comparison [:systems :left :name])
                  (get-in comparison [:systems :right :name])
                  (fixed (:left-median-ms row))
                  (fixed (:left-p95-ms row))
                  (fixed (:right-median-ms row))
                  (fixed (:right-p95-ms row))
                  (fixed (:right-over-left-median-ratio row))
                  (:lower-median-system row)
                  (fixed (:lower-median-factor row))
                  (pr-str (:result-counts row))
                  (:exact-result-digest-match? row)]))
          "\n"))))))

(defn- parse-args
  [args]
  (loop [remaining args
         paths []
         options {}]
    (if-let [arg (first remaining)]
      (case arg
        "--edn" (if-let [value (second remaining)]
                  (recur (nnext remaining) paths (assoc options :edn value))
                  (fail! "Missing value for --edn" {:option "--edn"}))
        "--csv" (if-let [value (second remaining)]
                  (recur (nnext remaining) paths (assoc options :csv value))
                  (fail! "Missing value for --csv" {:option "--csv"}))
        "--left-warmup" (if-let [value (second remaining)]
                          (recur (nnext remaining) paths
                                 (assoc options :left-warmup value))
                          (fail! "Missing value for --left-warmup"
                                 {:option "--left-warmup"}))
        "--right-warmup" (if-let [value (second remaining)]
                           (recur (nnext remaining) paths
                                  (assoc options :right-warmup value))
                           (fail! "Missing value for --right-warmup"
                                  {:option "--right-warmup"}))
        "--help" (recur (next remaining) paths (assoc options :help? true))
        (if (str/starts-with? arg "-")
          (fail! (str "Unknown option: " arg) {:option arg})
          (recur (next remaining) (conj paths arg) options)))
      (assoc options :paths paths))))

(defn usage
  []
  (str
    "Compare two correctness-gated LDBC harness reports.\n\n"
    "Usage:\n"
    "  clj -M:compare LEFT.edn RIGHT.edn [--edn comparison.edn] "
    "[--csv comparison.csv]\n"
    "      [--left-warmup LEFT-WARMUP.edn] "
    "[--right-warmup RIGHT-WARMUP.edn]\n"))

(defn -main
  [& args]
  (let [{:keys [paths edn csv help? left-warmup right-warmup]}
        (parse-args args)
        edn-path edn
        csv-path csv]
    (if help?
      (println (usage))
      (do
        (when-not (= 2 (count paths))
          (fail! "Exactly two report paths are required" {:paths paths}))
        (when (not= (boolean left-warmup) (boolean right-warmup))
          (fail! "Both --left-warmup and --right-warmup are required together"
                 {:left-warmup left-warmup :right-warmup right-warmup}))
        (let [[left-path right-path] paths
              left-report (edn/read-string (slurp left-path))
              right-report (edn/read-string (slurp right-path))
              comparison-base
              (compare-reports
                left-report
                right-report
                {:left-path (.getCanonicalPath (io/file left-path))
                 :right-path (.getCanonicalPath (io/file right-path))})
              comparison
              (if left-warmup
                (assoc
                  comparison-base
                  :independent-pass-validation
                  {:left
                   (verify-independent-pass
                     (edn/read-string (slurp left-warmup))
                     left-report
                     {:warmup-path
                      (.getCanonicalPath (io/file left-warmup))
                      :measurement-path
                      (.getCanonicalPath (io/file left-path))})
                   :right
                   (verify-independent-pass
                     (edn/read-string (slurp right-warmup))
                     right-report
                     {:warmup-path
                      (.getCanonicalPath (io/file right-warmup))
                      :measurement-path
                      (.getCanonicalPath (io/file right-path))})})
                comparison-base)]
          (when edn-path
            (ensure-parent! edn-path)
            (spit edn-path (with-out-str (pprint/pprint comparison))
                  :encoding "UTF-8"))
          (when csv-path
            (write-csv! csv-path comparison))
          (print (markdown comparison)))))))
