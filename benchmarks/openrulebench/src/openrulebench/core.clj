(ns openrulebench.core
  "Core benchmarking utilities for OpenRuleBench.
   Repeated wall-clock measurement, correctness oracles, and result artifacts."
  (:require
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [openrulebench.data :as data])
  (:import
   [java.time Instant]
   [java.util ArrayDeque ArrayList BitSet]))

;; ============================================================
;; Timing
;; ============================================================

(defn now-ms
  "Current time in milliseconds (high precision)."
  ^double []
  (/ (System/nanoTime) 1000000.0))

(defmacro time-once
  "Run body once and return [result time-ms]."
  [& body]
  `(let [start# (now-ms)
         result# (do ~@body)
         end# (now-ms)]
     [result# (- end# start#)]))

(defn percentile
  "Nearest-rank percentile over a non-empty collection."
  [sorted-values fraction]
  (let [n     (count sorted-values)
        index (min (dec n)
                   (dec (long (Math/ceil (* fraction n)))))]
    (nth sorted-values (max 0 index))))

(defn latency-summary
  [times-ms]
  (let [values (vec (sort times-ms))
        n      (count values)]
    (when (pos? n)
      {:count  n
       :min    (first values)
       :median (percentile values 0.5)
       :p95    (percentile values 0.95)
       :p99    (percentile values 0.99)
       :max    (peek values)
       :mean   (/ (reduce + values) (double n))})))

;; ============================================================
;; Independent TC/SG fixed-point references
;; ============================================================

(defn- node-count
  [relations]
  (let [nodes (seq (mapcat identity relations))]
    (if nodes
      (inc (long (reduce max nodes)))
      0)))

(defn- adjacency
  [n pairs direction]
  (let [result (object-array n)]
    (dotimes [i n]
      (aset result i (ArrayList.)))
    (doseq [[a b] pairs]
      (let [[from to] (if (= direction :forward) [a b] [b a])]
        (.add ^ArrayList (aget result (int from)) (long to))))
    result))

(defn tc-reference-count
  "Return the exact transitive-closure cardinality using independent BFS."
  [edges]
  (let [n   (node-count edges)
        adj (adjacency n edges :forward)]
    (loop [source 0
           total  0]
      (if (= source n)
        total
        (let [seen  (BitSet. n)
              queue (ArrayDeque.)]
          (doseq [target ^ArrayList (aget adj source)]
            (let [target (long target)]
              (when-not (.get seen (int target))
                (.set seen (int target))
                (.addLast queue target))))
          (while (not (.isEmpty queue))
            (let [node (long (.removeFirst queue))]
              (doseq [target ^ArrayList (aget adj (int node))]
                (let [target (long target)]
                  (when-not (.get seen (int target))
                    (.set seen (int target))
                    (.addLast queue target))))))
          (recur (inc source) (+ total (.cardinality seen))))))))

(defn sg-reference-count
  "Return the exact SG fixed-point cardinality using a delta work queue."
  [{:keys [par sib]}]
  (let [n     (node-count (concat par sib))
        preds (adjacency n par :reverse)
        seen  (object-array n)
        queue (ArrayDeque.)
        add!  (fn [x y]
                (let [^BitSet row (aget seen (int x))]
                  (when-not (.get row (int y))
                    (.set row (int y))
                    (.addLast queue (+ (* (long x) n) (long y)))
                    true)))]
    (dotimes [i n]
      (aset seen i (BitSet. n)))
    (doseq [[x y] sib]
      (add! x y))
    (while (not (.isEmpty queue))
      (let [pair (long (.removeFirst queue))
            z    (quot pair n)
            z1   (rem pair n)]
        (doseq [x ^ArrayList (aget preds (int z))
                y ^ArrayList (aget preds (int z1))]
          (add! x y))))
    (reduce (fn [total ^BitSet row] (+ total (.cardinality row)))
            0
            seen)))

(defn parse-benchmark
  [spec]
  (let [[bench-type instance & extra] (str/split spec #":" -1)]
    (when (or (seq extra) (str/blank? bench-type) (str/blank? instance))
      (throw (ex-info (str "Invalid benchmark specification: " spec)
                      {:benchmark spec})))
    [bench-type instance]))

(def ^:private expected-count-cache (atom {}))

(defn expected-result-count
  "Return an independent fixed-point count for TC/SG, or nil for workloads
   that do not yet have a suite oracle."
  [spec]
  (if (contains? @expected-count-cache spec)
    (get @expected-count-cache spec)
    (let [[bench-type instance] (parse-benchmark spec)
          expected
          (case bench-type
            "tc" (tc-reference-count
                   (data/generate-tc-instance (keyword instance)))
            "sg" (sg-reference-count
                   (data/generate-sg-instance (keyword instance)))
            nil)]
      (swap! expected-count-cache assoc spec expected)
      expected)))

;; ============================================================
;; Result formatting
;; ============================================================

(defn round
  "Round to reasonable precision."
  [^double n]
  (cond
    (> n 100)   (format "%.1f" n)
    (> n 10)    (format "%.2f" n)
    (> n 1)     (format "%.2f" n)
    (> n 0.1)   (format "%.3f" n)
    :else       (format "%.4f" n)))

(defn format-result
  "Format a benchmark result for display."
  [{:keys [benchmark time-ms result-count status]}]
  (case status
    :ok      (format "%s\t%s\t%d" benchmark (round time-ms) result-count)
    :timeout (format "%s\tT/O\t-" benchmark)
    :oom     (format "%s\tOOM\t-" benchmark)
    :incorrect (format "%s\tBAD\t-" benchmark)
    :unsupported (format "%s\tN/A\t-" benchmark)
    :error   (format "%s\tERR\t-" benchmark)
    (format "%s\t---\t-" benchmark)))

;; ============================================================
;; CSV output
;; ============================================================

(defn write-results-csv
  "Write benchmark results to CSV file."
  [results path]
  (io/make-parents path)
  (with-open [w (io/writer path)]
    (.write w "system,benchmark,time_ms,result_count,status\n")
    (doseq [{:keys [system benchmark time-ms result-count status]} results]
      (.write w (format "%s,%s,%s,%s,%s\n"
                        system
                        benchmark
                        (if (= status :ok) (round time-ms) "")
                        (if (= status :ok) result-count "")
                        (name status))))))

;; ============================================================
;; Console output
;; ============================================================

(defn print-header
  "Print benchmark header."
  [benchmarks]
  (println)
  (print "system\t\t")
  (doseq [b benchmarks]
    (print b "\t"))
  (println)
  (println (apply str (repeat 80 "-"))))

(defn print-row
  "Print results for one system."
  [system-name results]
  (print system-name "\t")
  (when (< (count system-name) 8) (print "\t"))
  (doseq [{:keys [time-ms status]} results]
    (case status
      :ok      (print (round time-ms) "\t")
      :timeout (print "T/O\t")
      :oom     (print "OOM\t")
      :incorrect (print "BAD\t")
      :unsupported (print "N/A\t")
      :error   (print "ERR\t")
      (print "---\t")))
  (println)
  (flush))

(defn print-summary
  "Print summary of all results."
  [all-results]
  (println)
  (println (apply str (repeat 80 "=")))
  (println "Summary")
  (println (apply str (repeat 80 "=")))
  (println)
  (println (format "%-12s %-15s %12s %12s %8s"
                   "System" "Benchmark" "Time (ms)" "Results" "Status"))
  (println (apply str (repeat 60 "-")))
  (doseq [{:keys [system benchmark time-ms result-count status]} all-results]
    (println (format "%-12s %-15s %12s %12s %8s"
                     system
                     benchmark
                     (if (= status :ok) (round time-ms) "-")
                     (if (= status :ok) (str result-count) "-")
                     (name status)))))

;; ============================================================
;; Repeated benchmark contract and child-runner CLI
;; ============================================================

(def default-run-options
  {:warmup 1 :iterations 5 :verify? true :quiet? false :output nil})

(defn- first-failure
  [results]
  (first (remove #(= :ok (:status %)) results)))

(defn- run-until-failure
  [run-once spec n]
  (loop [i 0
         results []]
    (if (= i n)
      results
      (let [result (run-once spec)
            results' (conj results result)]
        (if (= :ok (:status result))
          (recur (inc i) results')
          results')))))

(defn run-repeated
  [system spec run-once {:keys [warmup iterations verify?]}]
  (let [warmup-results (run-until-failure run-once spec warmup)]
    (if-let [failure (first-failure warmup-results)]
      (assoc failure
             :system system
             :benchmark spec
             :phase :warmup
             :samples-ms [])
      (let [runs (run-until-failure run-once spec iterations)]
        (if-let [failure (first-failure runs)]
          (assoc failure
                 :system system
                 :benchmark spec
                 :phase :measurement
                 :samples-ms (mapv :time-ms (take-while #(= :ok (:status %))
                                                        runs)))
          (let [counts       (mapv :result-count runs)
                consistent?  (apply = counts)
                expected     (when verify? (expected-result-count spec))
                oracle-ok?   (or (nil? expected)
                                 (every? #(= expected %) counts))
                times        (mapv :time-ms runs)
                summary      (latency-summary times)
                correctness  (cond
                               (not verify?)
                               {:status :skipped}

                               (some? expected)
                               {:status (if (and consistent? oracle-ok?)
                                          :passed :failed)
                                :oracle :independent-fixed-point
                                :expected-count expected}

                               :else
                               {:status (if consistent? :consistent :failed)
                                :oracle :none})]
            (if (and consistent? oracle-ok?)
              {:system system
               :benchmark spec
               :status :ok
               :result-count (first counts)
               :time-ms (:median summary)
               :latency-ms summary
               :samples-ms times
               :correctness correctness}
              {:system system
               :benchmark spec
               :status :incorrect
               :result-counts counts
               :expected-count expected
               :samples-ms times
               :correctness correctness})))))))

(defn- parse-positive-long
  [option value allow-zero?]
  (when-not value
    (throw (ex-info (str "Missing value for " option) {:option option})))
  (let [parsed (Long/parseLong value)]
    (when (if allow-zero? (neg? parsed) (not (pos? parsed)))
      (throw (ex-info
               (str option (if allow-zero?
                             " must not be negative"
                             " must be positive"))
               {:option option :value value})))
    parsed))

(defn parse-run-args
  [args default-benchmarks]
  (loop [remaining args
         opts default-run-options
         benchmarks []]
    (if-let [arg (first remaining)]
      (case arg
        "--warmup"
        (recur (nnext remaining)
               (assoc opts :warmup
                      (parse-positive-long arg (second remaining) true))
               benchmarks)

        "--iterations"
        (recur (nnext remaining)
               (assoc opts :iterations
                      (parse-positive-long arg (second remaining) false))
               benchmarks)

        "--output"
        (let [value (second remaining)]
          (when-not value
            (throw (ex-info "Missing value for --output" {:option arg})))
          (recur (nnext remaining) (assoc opts :output value) benchmarks))

        "--no-verify"
        (recur (next remaining) (assoc opts :verify? false) benchmarks)

        "--quiet"
        (recur (next remaining) (assoc opts :quiet? true) benchmarks)

        "--help"
        (recur (next remaining) (assoc opts :help? true) benchmarks)

        (if (str/starts-with? arg "--")
          (throw (ex-info (str "Unrecognized option: " arg) {:option arg}))
          (do
            (parse-benchmark arg)
            (recur (next remaining) opts (conj benchmarks arg)))))
      (assoc opts :benchmarks (if (seq benchmarks)
                                benchmarks
                                default-benchmarks)))))

(defn host-info
  []
  {:timestamp  (str (Instant/now))
   :clojure    (clojure-version)
   :java       (System/getProperty "java.version")
   :vm         (System/getProperty "java.vm.name")
   :os         (System/getProperty "os.name")
   :os-version (System/getProperty "os.version")
   :arch       (System/getProperty "os.arch")
   :processors (.availableProcessors (Runtime/getRuntime))})

(defn write-edn!
  [path value]
  (io/make-parents path)
  (spit path (with-out-str (pprint/pprint value))))

(defn child-usage
  [system]
  (str "OpenRuleBench " system " runner\n\n"
       "Options:\n"
       "  --warmup N       Fresh-database warmup runs (default 1)\n"
       "  --iterations N   Measured fresh-database runs (default 5)\n"
       "  --output PATH    Write an EDN report with raw samples\n"
       "  --no-verify      Skip the independent TC/SG count oracle\n"
       "  --quiet          Suppress the console result row\n"
       "  --help           Show this help\n"))

(defn result-failure?
  [{:keys [status]}]
  (contains? #{:error :timeout :oom :incorrect} status))

(defn run-system-cli!
  [system default-benchmarks run-once args]
  (let [{:keys [benchmarks output quiet? help?] :as opts}
        (parse-run-args args default-benchmarks)]
    (if help?
      (do
        (println (child-usage system))
        {:exit-code 0 :help? true})
      (let [results (mapv #(run-repeated system % run-once opts) benchmarks)
            report  {:format-version 1
                     :benchmark-suite :openrulebench
                     :system system
                     :host (host-info)
                     :configuration (select-keys opts
                                                 [:warmup :iterations :verify?])
                     :results results}
            exit-code (if (some result-failure? results) 1 0)]
        (when-not quiet?
          (print-row system results))
        (when output
          (write-edn! output report))
        (assoc report :exit-code exit-code)))))
