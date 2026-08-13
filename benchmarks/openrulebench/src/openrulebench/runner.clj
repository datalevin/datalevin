(ns openrulebench.runner
  "Process-isolated OpenRuleBench orchestrator."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [openrulebench.core :as core]))

(def default-benchmarks ["tc:small" "sg:small"])

(def all-benchmarks
  ["tc:small" "tc:medium" "tc:large" "tc:xlarge"
   "sg:small" "sg:medium" "sg:large"
   "join1:small" "join1:medium" "join1:large"
   "dblp:small" "dblp:medium" "dblp:large"
   "lubm:lubm-1" "lubm:lubm-10" "lubm:lubm-50"])

(def stress-benchmarks
  ["tc:xxlarge" "join1:large" "lubm:lubm-50"])

(def default-systems [:datalevin :sqlite])

(def system-config
  {:datalevin {:alias "datalevin"
               :namespace "openrulebench.datalevin"
               :heap? true
               :workloads #{"tc" "sg" "join1" "dblp" "lubm"}}
   :sqlite    {:alias "sqlite"
               :namespace "openrulebench.sqlite"
               :workloads #{"tc" "sg"}}
   :postgresql {:alias "postgresql"
                :namespace "openrulebench.postgresql"
                :workloads #{"tc" "sg"}}
   :xsb       {:alias "xsb"
               :namespace "openrulebench.xsb"
               :workloads #{"tc" "sg"}}
   :souffle   {:alias "souffle"
               :namespace "openrulebench.souffle"
               :workloads #{"tc" "sg"}}
   :clara     {:alias "clara"
               :namespace "openrulebench.clara"
               :heap? true
               :workloads #{"tc" "sg"}}
   :odoyle    {:alias "odoyle"
               :namespace "openrulebench.odoyle"
               :heap? true
               :workloads #{"tc" "sg"}}})

(def default-options
  {:systems default-systems
   :warmup 1
   :iterations 5
   :verify? true
   :output nil})

(defn- parse-systems
  [value]
  (when-not value
    (throw (ex-info "Missing value for --systems" {:option "--systems"})))
  (let [systems (->> (str/split value #",")
                     (remove str/blank?)
                     (mapv keyword))
        unknown (seq (remove system-config systems))]
    (when (empty? systems)
      (throw (ex-info "--systems must not be empty" {:value value})))
    (when unknown
      (throw (ex-info (str "Unknown systems: "
                           (str/join "," (map name unknown)))
                      {:systems unknown})))
    systems))

(defn- parse-count
  [option value allow-zero?]
  (when-not value
    (throw (ex-info (str "Missing value for " option) {:option option})))
  (let [n (Long/parseLong value)]
    (when (if allow-zero? (neg? n) (not (pos? n)))
      (throw (ex-info
               (str option (if allow-zero?
                             " must not be negative"
                             " must be positive"))
               {:option option :value value})))
    n))

(defn- resolve-benchmarks
  [benchmarks]
  (cond
    (empty? benchmarks) default-benchmarks
    (= ["all"] benchmarks) all-benchmarks
    (= ["stress"] benchmarks) stress-benchmarks
    :else (do
            (doseq [spec benchmarks]
              (core/parse-benchmark spec))
            benchmarks)))

(defn parse-args
  [args]
  (loop [remaining args
         opts default-options
         benchmarks []]
    (if-let [arg (first remaining)]
      (case arg
        "--systems"
        (recur (nnext remaining)
               (assoc opts :systems (parse-systems (second remaining)))
               benchmarks)

        "--system"
        (recur (nnext remaining)
               (assoc opts :systems (parse-systems (second remaining)))
               benchmarks)

        "--warmup"
        (recur (nnext remaining)
               (assoc opts :warmup
                      (parse-count arg (second remaining) true))
               benchmarks)

        "--iterations"
        (recur (nnext remaining)
               (assoc opts :iterations
                      (parse-count arg (second remaining) false))
               benchmarks)

        "--output"
        (let [value (second remaining)]
          (when-not value
            (throw (ex-info "Missing value for --output" {:option arg})))
          (recur (nnext remaining) (assoc opts :output value) benchmarks))

        "--no-verify"
        (recur (next remaining) (assoc opts :verify? false) benchmarks)

        "--help"
        (recur (next remaining) (assoc opts :help? true) benchmarks)

        (if (str/starts-with? arg "--")
          (throw (ex-info (str "Unrecognized option: " arg) {:option arg}))
          (recur (next remaining) opts (conj benchmarks arg))))
      (assoc opts :benchmarks (resolve-benchmarks benchmarks)))))

(defn usage
  []
  (str
    "OpenRuleBench benchmark harness\n\n"
    "Usage: ./bench.clj [options] [benchmark ... | all | stress]\n\n"
    "Options:\n"
    "  --systems LIST    Comma-separated systems (default datalevin,sqlite)\n"
    "  --warmup N        Fresh-database warmup runs (default 1)\n"
    "  --iterations N    Measured fresh-database runs (default 5)\n"
    "  --output PATH     Write combined metadata and raw samples as EDN\n"
    "  --no-verify       Skip independent TC/SG result-count oracles\n"
    "  --help            Show this help\n\n"
    "Systems: datalevin, sqlite, postgresql, xsb, souffle, clara, odoyle\n"
    "Benchmark syntax: tc:tiny, sg:small, join1:small, dblp:small, ...\n"))

(defn- supported?
  [system spec]
  (let [[workload _] (core/parse-benchmark spec)]
    (contains? (get-in system-config [system :workloads]) workload)))

(defn- child-command
  [system opts output supported]
  (let [{:keys [alias namespace heap?]} (system-config system)]
    (vec
      (concat
        ["clojure"]
        (when heap? ["-J-Xmx8g"])
        [(str "-M:" alias) "-m" namespace
         "--warmup" (str (:warmup opts))
         "--iterations" (str (:iterations opts))
         "--output" output
         "--quiet"]
        (when-not (:verify? opts) ["--no-verify"])
        supported))))

(defn- error-result
  [system spec message]
  {:system (name system)
   :benchmark spec
   :status :error
   :error message})

(defn- run-child
  [system opts supported]
  (let [file (java.io.File/createTempFile
               (str "openrulebench-" (name system) "-") ".edn")
        path (.getAbsolutePath file)
        cmd  (child-command system opts path supported)]
    (try
      (let [{:keys [exit out err]} (apply shell/sh (concat cmd [:dir "."]))
            report (when (pos? (.length file))
                     (edn/read-string (slurp file)))]
        (when-not (str/blank? out)
          (print out))
        (when-not (str/blank? err)
          (binding [*out* *err*]
            (print err)))
        (if report
          (cond-> report
            (not= exit 0) (assoc :child-exit exit))
          {:system (name system)
           :child-exit exit
           :results (mapv #(error-result system %
                                        (str "child process exited " exit))
                          supported)}))
      (catch Exception e
        {:system (name system)
         :results (mapv #(error-result system % (.getMessage e)) supported)})
      (finally
        (io/delete-file file true)))))

(defn- order-results
  [system benchmarks child-results]
  (let [by-benchmark (into {} (map (juxt :benchmark identity)) child-results)]
    (mapv
      (fn [spec]
        (if (supported? system spec)
          (or (get by-benchmark spec)
              (error-result system spec "child result was missing"))
          {:system (name system)
           :benchmark spec
           :status :unsupported
           :reason "workload is not implemented for this system"}))
      benchmarks)))

(defn- run-system
  [system {:keys [benchmarks] :as opts}]
  (let [supported (filterv #(supported? system %) benchmarks)
        child     (when (seq supported) (run-child system opts supported))
        results   (order-results system benchmarks (:results child))]
    {:system system
     :host (:host child)
     :child-exit (:child-exit child)
     :results results}))

(defn run-suite
  [{:keys [systems benchmarks output] :as opts}]
  (let [system-reports
        (mapv
          (fn [system]
            (println "Running" (name system) "...")
            (run-system system opts))
          systems)
        results (vec (mapcat :results system-reports))
        report  {:format-version 1
                 :benchmark-suite :openrulebench
                 :host (core/host-info)
                 :configuration (select-keys opts
                                             [:systems :benchmarks :warmup
                                              :iterations :verify?])
                 :system-hosts (into {} (map (juxt :system :host)) system-reports)
                 :results results}
        child-failed? (some #(not (contains? #{nil 0} (:child-exit %)))
                            system-reports)
        exit-code (if (or child-failed?
                          (some core/result-failure? results))
                    1
                    0)]
    (core/print-header benchmarks)
    (doseq [{:keys [system results]} system-reports]
      (core/print-row (name system) results))
    (when output
      (core/write-edn! output report)
      (println)
      (println "Wrote results to" output))
    (assoc report :exit-code exit-code)))

(defn main!
  [args]
  (let [opts (parse-args args)]
    (if (:help? opts)
      (do (println (usage)) 0)
      (:exit-code (run-suite opts)))))
