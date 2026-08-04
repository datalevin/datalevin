(ns datalevin-bench.external-analysis
  "Validation and summary for the repeated CIDR external-viability run."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [datalevin-bench.external-benchmark :as external]
   [datalevin.core :as d]))

(defn- read-csv
  [path]
  (with-open [reader (io/reader path)]
    (let [[header & rows] (doall (d/read-csv reader))]
      (mapv #(zipmap header %) rows))))

(defn- read-edn-forms
  [path]
  (with-open [reader (java.io.PushbackReader. (io/reader path))]
    (loop [forms []]
      (let [form (edn/read {:eof ::eof} reader)]
        (if (= ::eof form)
          forms
          (recur (conj forms form)))))))

(defn- parse-row
  [row]
  {:run (parse-long (row "Run"))
   :schedule-seed (parse-long (row "Schedule Seed"))
   :position (parse-long (row "Position"))
   :system (keyword (row "System"))
   :query (row "Query Name")
   :sample-seed (when (seq (row "Query Sample Seed"))
                  (parse-long (row "Query Sample Seed")))
   :planning-ms (when (seq (row "Planning Time (ms)"))
                  (parse-double (row "Planning Time (ms)")))
   :execution-ms (when (seq (row "Execution Time (ms)"))
                   (parse-double (row "Execution Time (ms)")))
   :result-size (when (seq (row "Result Size"))
                  (parse-long (row "Result Size")))
   :status (keyword (row "Status"))
   :error (not-empty (row "Error"))})

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
     :swapout-deltas (mapv #(get-in % [:vm-delta :swapouts]) after)}))

(defn- median
  [values]
  (let [values (vec (sort values))
        n      (count values)
        mid    (quot n 2)]
    (when (pos? n)
      (if (odd? n)
        (double (values mid))
        (/ (+ (double (values (dec mid)))
              (double (values mid)))
           2.0)))))

(defn- run-totals
  [timing]
  (mapv
    (fn [[run rows]]
      (let [successful (filter #(= :ok (:status %)) rows)
            planning   (reduce + (keep :planning-ms successful))
            execution  (reduce + (keep :execution-ms successful))]
        {:run run
         :rows (count rows)
         :completed (count successful)
         :planning-ms planning
         :execution-ms execution
         :total-ms (+ planning execution)}))
    (sort-by key (group-by :run timing))))

(defn- distribution
  [values]
  {:min (when (seq values) (apply min values))
   :median (median values)
   :max (when (seq values) (apply max values))})

(defn- system-summary
  [timing]
  (let [totals (run-totals timing)]
    {:rows (count timing)
     :queries (count (distinct (map :query timing)))
     :runs (count totals)
     :statuses (frequencies (map :status timing))
     :run-totals totals
     :planning-ms (distribution (map :planning-ms totals))
     :execution-ms (distribution (map :execution-ms totals))
     :total-ms (distribution (map :total-ms totals))}))

(defn- valid-run?
  [queries rows]
  (and (= (count queries) (count rows))
       (= (set queries) (set (map :query rows)))
       (= (set (range (count queries)))
          (set (map :position rows)))
       (= 1 (count (set (map :schedule-seed rows))))))

(defn- artifact-validation
  [expected-system {:keys [timing health manifests]}]
  (let [manifest       (last manifests)
        config         (:config manifest)
        queries        (:queries config)
        expected-rows  (* (long (:runs config)) (count queries))
        rows-by-run    (group-by :run timing)
        run-errors     (->> rows-by-run
                            (keep
                              (fn [[run rows]]
                                (when-not (valid-run? queries rows)
                                  {:run run
                                   :rows (count rows)
                                   :queries
                                   (count (distinct (map :query rows)))})))
                            vec)
        sample-errors
        (if (= :datalevin expected-system)
          (->> timing
               (keep
                 (fn [{:keys [schedule-seed query sample-seed] :as row}]
                   (let [expected
                         (external/query-sample-seed schedule-seed query)]
                     (when (not= expected sample-seed)
                       (select-keys
                         (assoc row :expected-sample-seed expected)
                         [:run :query :sample-seed
                          :expected-sample-seed])))))
               vec)
          (->> timing
               (keep #(when (:sample-seed %) (select-keys % [:run :query])))
               vec))
        health-summary (health-summary health)
        statuses       (frequencies (map :status timing))
        accepted?
        (boolean
          (and (= :complete (:status manifest))
               (= expected-system (:system config))
               (= expected-rows (count timing))
               (= (:runs config) (count rows-by-run))
               (empty? run-errors)
               (empty? sample-errors)
               (= {:ok expected-rows} statuses)
               (:paired-passes? health-summary)
               (= (inc (:runs config)) (:after-records health-summary))
               (zero? (:failed-count health-summary))
               (zero? (:contaminated-count health-summary))
               (zero? (:docker-detected-count health-summary))
               (zero? (:swapout-pass-count health-summary))))]
    {:accepted? accepted?
     :system expected-system
     :manifest-status (:status manifest)
     :expected-rows expected-rows
     :actual-rows (count timing)
     :statuses statuses
     :run-error-count (count run-errors)
     :run-errors (take 10 run-errors)
     :sample-error-count (count sample-errors)
     :sample-errors (take 10 sample-errors)
     :health health-summary}))

(defn- schedule-key
  [row]
  [(:run row) (:position row)])

(defn- schedule-validation
  [postgres datalevin]
  (let [pg-by-key (group-by schedule-key postgres)
        dl-by-key (group-by schedule-key datalevin)
        keys      (into (set (keys pg-by-key)) (keys dl-by-key))
        errors
        (->> keys
             (keep
               (fn [key]
                 (let [pg-rows (pg-by-key key)
                       dl-rows (dl-by-key key)
                       pg      (first pg-rows)
                       dl      (first dl-rows)]
                   (when (or (not= 1 (count pg-rows))
                             (not= 1 (count dl-rows))
                             (not= (:schedule-seed pg) (:schedule-seed dl))
                             (not= (:query pg) (:query dl)))
                     {:key key
                      :postgres
                      (select-keys pg [:schedule-seed :query])
                      :datalevin
                      (select-keys dl [:schedule-seed :query])}))))
             vec)]
    {:pairs (count keys)
     :mismatch-count (count errors)
     :mismatches (take 10 errors)}))

(defn- paired-ratios
  [postgres-summary datalevin-summary]
  (let [pg (into {} (map (juxt :run identity))
                 (:run-totals postgres-summary))
        dl (into {} (map (juxt :run identity))
                 (:run-totals datalevin-summary))
        ratios
        (mapv
          (fn [run]
            {:run run
             :postgres-over-datalevin
             (/ (double (:total-ms (pg run)))
                (double (:total-ms (dl run))))})
          (sort (into (set (keys pg)) (keys dl))))]
    {:runs ratios
     :median-postgres-over-datalevin
     (median (map :postgres-over-datalevin ratios))}))

(defn analyze-artifacts
  [{:keys [postgres datalevin]}]
  (let [postgres-validation (artifact-validation :postgres postgres)
        datalevin-validation (artifact-validation :datalevin datalevin)
        schedules (schedule-validation (:timing postgres)
                                       (:timing datalevin))
        accepted? (and (:accepted? postgres-validation)
                       (:accepted? datalevin-validation)
                       (zero? (:mismatch-count schedules)))
        postgres-summary (system-summary (:timing postgres))
        datalevin-summary (system-summary (:timing datalevin))]
    {:accepted? accepted?
     :validation
     {:postgres postgres-validation
      :datalevin datalevin-validation
      :schedules schedules}
     :summary
     {:postgres postgres-summary
      :datalevin datalevin-summary
      :paired-ratio (paired-ratios postgres-summary datalevin-summary)}}))

(defn- load-artifact
  [timing-file health-file manifest-file]
  {:timing (mapv parse-row (read-csv timing-file))
   :health (read-edn-forms health-file)
   :manifests (read-edn-forms manifest-file)})

(defn run
  "Validate and summarize exact PostgreSQL and Datalevin external artifacts."
  [{:keys [postgres-timing-file postgres-health-file postgres-manifest-file
           datalevin-timing-file datalevin-health-file
           datalevin-manifest-file output-dir]}]
  (let [report
        (analyze-artifacts
          {:postgres
           (load-artifact postgres-timing-file
                          postgres-health-file
                          postgres-manifest-file)
           :datalevin
           (load-artifact datalevin-timing-file
                          datalevin-health-file
                          datalevin-manifest-file)})
        stamp       (System/currentTimeMillis)
        output-dir  (io/file (or output-dir
                                 "results/cidr-external-comparison"))
        output-file (io/file output-dir
                             (str "external_summary_" stamp ".edn"))]
    (.mkdirs output-dir)
    (spit output-file (str (pr-str report) "\n"))
    (println "External benchmark validation:"
             {:accepted? (:accepted? report)
              :output-file (.getPath output-file)})
    (when-not (:accepted? report)
      (throw (ex-info "External benchmark artifacts failed validation"
                      {:output-file (.getPath output-file)
                       :report report})))
    {:accepted? true :output-file (.getPath output-file)}))
