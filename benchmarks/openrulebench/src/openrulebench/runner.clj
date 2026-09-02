(ns openrulebench.runner
  "Process-isolated OpenRuleBench orchestrator."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [openrulebench.core :as core]))

(def binding-names ["ff" "bf" "fb"])
(def graph-shape-names ["cyclic" "acyclic"])

(defn- graph-task-specs
  [family sizes]
  (vec (for [size sizes
             shape graph-shape-names
             binding binding-names]
         (str family ":" size "-" shape "-" binding))))

(defn- join1-task-specs
  [sizes]
  (vec (for [size sizes
             query ["a" "b1" "b2"]
             binding binding-names]
         (str "join1:" size "-" query "-" binding))))

(def smoke-benchmarks ["tc:tiny" "sg:tiny" "join1:tiny"])

(def differential-benchmarks
  (vec (concat (graph-task-specs "tc" ["tiny"])
               (graph-task-specs "sg" ["tiny"])
               (join1-task-specs ["tiny"]))))

(def default-benchmarks
  ["tc:50k-cyclic-ff" "tc:50k-acyclic-ff"
   "sg:6k-cyclic-ff" "sg:6k-acyclic-ff"])

(def recursive-benchmarks
  (vec (concat (graph-task-specs "tc" ["50k" "500k"])
               (graph-task-specs "sg" ["6k" "24k"]))))

(def join-benchmarks (join1-task-specs ["50k" "250k"]))

(def all-benchmarks (vec (concat recursive-benchmarks join-benchmarks)))

(def stress-benchmarks
  (vec (concat (graph-task-specs "tc" ["500k"])
               (graph-task-specs "sg" ["24k"])
               (join1-task-specs ["250k"]))))

(def benchmark-groups
  {"smoke" smoke-benchmarks
   "differential" differential-benchmarks
   "default" default-benchmarks
   "recursive" recursive-benchmarks
   "joins" join-benchmarks
   "stress" stress-benchmarks
   "all" all-benchmarks})

(def default-systems [:datalevin :sqlite])

(def system-config
  {:datalevin {:alias "datalevin"
               :namespace "openrulebench.datalevin"
               :families #{:tc :sg :join1}
               :bindings #{:ff :bf :fb}}
   :sqlite    {:alias "sqlite"
               :namespace "openrulebench.sqlite"
               :families #{:tc :sg :join1}
               :bindings #{:ff :bf :fb}}
   :postgresql {:alias "postgresql"
                :namespace "openrulebench.postgresql"
                :families #{:tc :sg :join1}
                :bindings #{:ff :bf :fb}}
   :xsb       {:alias "xsb"
               :namespace "openrulebench.xsb"
               :families #{:tc :sg :join1}
               :bindings #{:ff :bf :fb}}
   :souffle   {:alias "souffle"
               :namespace "openrulebench.souffle"
               :families #{:tc :sg :join1}
               :bindings #{:ff :bf :fb}}
   :clara     {:alias "clara"
               :namespace "openrulebench.clara"
               :families #{:tc :sg}
               :bindings #{:ff}}
   :odoyle    {:alias "odoyle"
               :namespace "openrulebench.odoyle"
               :families #{:tc :sg}
               :bindings #{:ff}
               ;; O'Doyle's rule firing does not respond to future
               ;; cancellation.  Keep each task in its own JVM so a timeout
               ;; cannot leave a worker consuming CPU while the next task
               ;; starts.
               :isolate-tasks? true}})

(def default-options
  {:systems default-systems
   :warmup 1
   :iterations 1
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
  (if (empty? benchmarks)
    default-benchmarks
    (if (and (= 1 (count benchmarks))
             (contains? benchmark-groups (first benchmarks)))
      (get benchmark-groups (first benchmarks))
      (do
        (doseq [spec benchmarks]
          (core/require-benchmark-task spec))
        benchmarks))))

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
    "Usage: ./bench.clj [options] [benchmark ... | GROUP]\n\n"
    "Options:\n"
    "  --systems LIST    Comma-separated systems (default datalevin,sqlite)\n"
    "  --warmup N        Complete warmup passes (default 1)\n"
    "  --iterations N    Complete measured passes (default 1)\n"
    "  --output PATH     Write combined metadata and raw measurements as EDN\n"
    "  --no-verify       Skip independent result-count oracles\n"
    "  --help            Show this help\n\n"
    "Systems: datalevin, sqlite, postgresql, xsb, souffle, clara, odoyle\n"
    "Groups: default, smoke, differential, recursive, joins, stress, all\n"
    "Task syntax:\n"
    "  tc:<50k|500k>-<cyclic|acyclic>-<ff|bf|fb>\n"
    "  sg:<6k|24k>-<cyclic|acyclic>-<ff|bf|fb>\n"
    "  join1:<50k|250k>-<a|b1|b2>-<ff|bf|fb>\n"))

(defn- supported?
  [system spec]
  (let [{:keys [family binding]} (core/require-benchmark-task spec)
        {:keys [families bindings]} (system-config system)]
    (and (contains? families family)
         (contains? bindings binding))))

(defn- child-command
  [system opts output supported]
  (let [{:keys [alias namespace]} (system-config system)]
    (vec
      (concat
        ["clojure" "-J-Xms2g" "-J-Xmx8g"]
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

(defn- child-task-groups
  [system supported]
  (if (get-in system-config [system :isolate-tasks?])
    (mapv vector supported)
    [supported]))

(defn- run-child-groups
  [system opts task-groups]
  (mapv #(run-child system opts %) task-groups))

(defn- nonzero-child-exit
  [children]
  (some (fn [{:keys [child-exit]}]
          (when (and child-exit (not= 0 child-exit))
            child-exit))
        children))

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
           :reason "task is outside this system's declared benchmark capabilities"}))
      benchmarks)))

(defn- run-system
  [system {:keys [benchmarks] :as opts}]
  (let [supported  (filterv #(supported? system %) benchmarks)
        children   (when (seq supported)
                     (run-child-groups
                       system opts (child-task-groups system supported)))
        child-exit      (nonzero-child-exit children)
        results         (order-results
                          system benchmarks
                          (mapcat :results children))]
    {:system system
     :host (:host (first children))
     :child-hosts (mapv :host children)
     :child-exit child-exit
     :results results}))

(defn- input-mismatches
  [results]
  (->> results
       (filter #(and (= :ok (:status %)) (:input-digest %)))
       (group-by :benchmark)
       (keep (fn [[benchmark task-results]]
               (let [digests (set (map :input-digest task-results))]
                 (when (> (count digests) 1)
                   {:benchmark benchmark :digests digests}))))
       vec))

(defn run-suite
  [{:keys [systems benchmarks output] :as opts}]
  (let [system-reports
        (mapv
          (fn [system]
            (println "Running" (name system) "...")
            (run-system system opts))
          systems)
        results (vec (mapcat :results system-reports))
        mismatches (input-mismatches results)
        report  {:format-version 2
                 :benchmark-suite :openrulebench
                 :contract :portable-tc-sg-join1-v1
                 :host (core/host-info)
                 :measurement-protocol
                 {:style :job-pass
                  :order :warmup-passes-then-measurement-passes
                  :pass-process :same-child-jvm
                  :reported-statistic
                  (if (= 1 (:iterations opts))
                    :single-measurement :median)}
                 :configuration (select-keys opts
                                             [:systems :benchmarks :warmup
                                              :iterations :verify?])
                 :cross-system-inputs {:status (if (seq mismatches)
                                                 :failed :passed)
                                       :mismatches mismatches}
                 :system-hosts (into {} (map (juxt :system :host)) system-reports)
                 :system-child-hosts
                 (into {}
                       (map (juxt :system :child-hosts))
                       system-reports)
                 :results results}
        child-failed? (some #(not (contains? #{nil 0} (:child-exit %)))
                            system-reports)
        exit-code (if (or (seq mismatches)
                          child-failed?
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
