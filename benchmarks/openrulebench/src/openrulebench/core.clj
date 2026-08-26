(ns openrulebench.core
  "Core benchmarking utilities for OpenRuleBench.
   Warmup/measurement pass execution, correctness oracles, and result artifacts."
  (:require
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [openrulebench.data :as data])
  (:import
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.security MessageDigest]
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

(defn- relation-row-count
  [n ^objects rows binding bound]
  (case binding
    :ff (reduce (fn [total ^BitSet row]
                  (+ total (.cardinality row)))
                0
                rows)
    :bf (if (and (<= 0 bound) (< bound n))
          (.cardinality ^BitSet (aget rows (int bound)))
          0)
    :fb (if (and (<= 0 bound) (< bound n))
          (reduce (fn [total ^BitSet row]
                    (if (.get row (int bound)) (inc total) total))
                  0
                  rows)
          0)))

(defn- tc-reference-rows
  [edges]
  (let [n    (node-count edges)
        rows (object-array n)]
    (dotimes [i n]
      (aset rows i (BitSet. n)))
    (doseq [[a b] edges]
      (.set ^BitSet (aget rows (int a)) (int b)))
    ;; BitSet Warshall is independent of every backend and avoids repeating a
    ;; dense adjacency scan once per source on the 500K case.
    (dotimes [k n]
      (let [^BitSet via (aget rows k)]
        (dotimes [i n]
          (let [^BitSet row (aget rows i)]
            (when (.get row k)
              (.or row via))))))
    {:size n :rows rows}))

(defn tc-reference-count
  "Return an exact TC cardinality using independent BitSet closure. Binding is
   one of :ff, :bf (first argument fixed), or :fb (second argument fixed)."
  ([edges]
   (tc-reference-count edges :ff 1))
  ([edges binding bound]
   (let [{:keys [size rows]} (tc-reference-rows edges)]
     (relation-row-count size rows binding bound))))

(defn- sg-reference-rows
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
    {:size n :rows seen}))

(defn sg-reference-count
  "Return an exact SG fixed-point cardinality using an independent delta work
   queue. Binding has the same :ff/:bf/:fb meaning as TC."
  ([relations]
   (sg-reference-count relations :ff 1))
  ([relations binding bound]
   (let [{:keys [size rows]} (sg-reference-rows relations)]
     (relation-row-count size rows binding bound))))

(defn parse-benchmark
  [spec]
  (let [[bench-type instance & extra] (str/split spec #":" -1)]
    (when (or (seq extra) (str/blank? bench-type) (str/blank? instance))
      (throw (ex-info (str "Invalid benchmark specification: " spec)
                      {:benchmark spec})))
    [bench-type instance]))

(def ^:private binding-modes #{:ff :bf :fb})
(def ^:private graph-shapes #{:cyclic :acyclic})

(defn- legacy-graph-task
  [family instance]
  (let [instances (case family
                    :tc data/tc-instances
                    :sg data/sg-instances)]
    (when (contains? instances (keyword instance))
      {:family family
       :instance (keyword instance)
       :shape :cyclic
       :binding :ff
       :bound-value 1
       :published? false})))

(defn- task-input-profile
  [{:keys [family instance]}]
  (case family
    :tc (let [{:keys [nodes edges]} (get data/tc-instances instance)]
          {:generator-version 1
           :seed 42
           :nodes nodes
           :edge-facts edges})
    :sg (let [{:keys [nodes par-facts sib-facts]}
              (get data/sg-instances instance)]
          {:generator-version 1
           :par-seed 42
           :sib-seed 43
           :nodes nodes
           :par-facts par-facts
           :sib-facts sib-facts})
    :join1 (let [{:keys [tuples domain]}
                 (get data/join1-instances instance)]
             {:generator-version 1
              :relation-seeds {:d1 1 :d2 2 :c2 3 :c3 4 :c4 5}
              :domain domain
              :facts-per-relation tuples})))

(defn benchmark-task
  "Parse a portable benchmark specification into an executable task map.

   Published forms are:
   tc:<50k|500k>-<cyclic|acyclic>-<ff|bf|fb>
   sg:<6k|24k>-<cyclic|acyclic>-<ff|bf|fb>
   join1:<50k|250k>-<a|b1|b2>-<ff|bf|fb>

   `tiny` may replace a published size in any of these forms for non-published
   differential runs. The old two-part instance names remain development
   aliases. DBLP, LUBM, and ORE are intentionally outside this portable
   comparison contract."
  [spec]
  (let [[workload instance-name] (parse-benchmark spec)
        parts (str/split instance-name #"-")
        task
        (case workload
          "tc"
          (if (= 3 (count parts))
            (let [[size shape binding] (map keyword parts)]
              (when (and (#{:tiny :50k :500k} size)
                         (graph-shapes shape)
                         (binding-modes binding))
                {:family :tc
                 :instance size
                 :shape shape
                 :binding binding
                 :bound-value 1
                 :published? (not= size :tiny)}))
            (legacy-graph-task :tc instance-name))

          "sg"
          (if (= 3 (count parts))
            (let [[size shape binding] (map keyword parts)]
              (when (and (#{:tiny :6k :24k} size)
                         (graph-shapes shape)
                         (binding-modes binding))
                {:family :sg
                 :instance size
                 :shape shape
                 :binding binding
                 :bound-value 1
                 :published? (not= size :tiny)}))
            (legacy-graph-task :sg instance-name))

          "join1"
          (if (= 3 (count parts))
            (let [[size query binding] (map keyword parts)]
              (when (and (#{:tiny :50k :250k} size)
                         (#{:a :b1 :b2} query)
                         (binding-modes binding))
                {:family :join1
                 :instance size
                 :query query
                 :binding binding
                 :bound-value 1
                 :published? (not= size :tiny)}))
            (when (contains? data/join1-instances (keyword instance-name))
              {:family :join1
               :instance (keyword instance-name)
               :query :a
               :binding :ff
               :bound-value 1
               :published? false}))

          nil)]
    (when task
      (assoc task
             :spec spec
             :input-profile (task-input-profile task)))))

(defn require-benchmark-task
  [spec]
  (or (benchmark-task spec)
      (throw (ex-info (str "Unknown or non-portable benchmark task: " spec)
                      {:benchmark spec}))))

(defn generate-task-data
  "Generate the canonical base relations for a parsed portable task."
  [{:keys [family instance shape]}]
  (case family
    :tc (data/generate-tc-instance instance
                                   {:acyclic? (= shape :acyclic)})
    :sg (data/generate-sg-instance instance
                                   {:acyclic? (= shape :acyclic)})
    :join1 (data/generate-join1-instance instance)
    (throw (ex-info "Task has no portable data generator" {:family family}))))

(defn task-base-fact-count
  [{:keys [family]} task-data]
  (case family
    :tc (count task-data)
    :sg (+ (count (:par task-data)) (count (:sib task-data)))
    :join1 (reduce + (map #(count (get task-data %))
                         [:d1 :d2 :c2 :c3 :c4]))))

(defn task-data-digest
  "Return a canonical SHA-256 digest of all named base-relation tuples."
  [{:keys [family]} task-data]
  (let [relations (case family
                    :tc [[:edge task-data]]
                    :sg [[:par (:par task-data)] [:sib (:sib task-data)]]
                    :join1 (mapv (fn [relation]
                                   [relation (get task-data relation)])
                                 [:d1 :d2 :c2 :c3 :c4]))
        digest (MessageDigest/getInstance "SHA-256")
        buffer (ByteBuffer/allocate 16)
        bytes  (.array buffer)]
    (doseq [[relation pairs] relations]
      (.update digest (.getBytes (name relation) StandardCharsets/UTF_8))
      (.update digest (byte 0))
      (doseq [[a b] pairs]
        (.clear buffer)
        (.putLong buffer (long a))
        (.putLong buffer (long b))
        (.update digest bytes)))
    (apply str (map #(format "%02x" (bit-and (int %) 0xff))
                    (.digest digest)))))

(defn- pairs->bit-rows
  [n pairs]
  (let [rows (object-array n)]
    (dotimes [i n]
      (aset rows i (BitSet. n)))
    (doseq [[a b] pairs]
      (.set ^BitSet (aget rows (int a)) (int b)))
    rows))

(defn- compose-bit-relations
  [n ^objects left ^objects right]
  (let [result (object-array n)]
    (dotimes [x n]
      (let [out (BitSet. n)
            ^BitSet intermediates (aget left x)]
        (loop [z (.nextSetBit intermediates 0)]
          (when (not= z -1)
            (.or out ^BitSet (aget right z))
            (recur (.nextSetBit intermediates (inc z)))))
        (aset result x out)))
    result))

(defn- join1-reference-relations
  [relations]
  (let [n   (inc (long (reduce max -1 (mapcat identity
                                                (mapcat #(get relations %)
                                                        [:d1 :d2 :c2 :c3
                                                         :c4])))))
        d1  (pairs->bit-rows n (:d1 relations))
        d2  (pairs->bit-rows n (:d2 relations))
        c2  (pairs->bit-rows n (:c2 relations))
        c3  (pairs->bit-rows n (:c3 relations))
        c4  (pairs->bit-rows n (:c4 relations))
        c1  (compose-bit-relations n d1 d2)
        b2  (compose-bit-relations n c3 c4)
        b1  (compose-bit-relations n c1 c2)
        a   (compose-bit-relations n b1 b2)]
    {:size n :relations {:a a :b1 b1 :b2 b2}}))

(defn join1-reference-count
  "Return an exact set-semantics JOIN1 answer count using independent BitSet
   relation composition."
  [relations query binding bound]
  (let [{:keys [size relations]} (join1-reference-relations relations)]
    (relation-row-count size (get relations query) binding bound)))

(def ^:private expected-count-cache (atom {}))
(def ^:private reference-cache (atom {}))

(defn- reference-for-task
  [{:keys [family instance shape] :as task}]
  (let [key (case family
              :tc [family instance shape]
              :sg [family instance shape]
              :join1 [family instance])]
    (if (contains? @reference-cache key)
      (get @reference-cache key)
      (let [task-data (generate-task-data task)
            reference (case family
                        :tc (tc-reference-rows task-data)
                        :sg (sg-reference-rows task-data)
                        :join1 (join1-reference-relations task-data))]
        (swap! reference-cache assoc key reference)
        reference))))

(defn expected-result-count
  "Return the independent answer cardinality for every portable task."
  [spec]
  (if (contains? @expected-count-cache spec)
    (get @expected-count-cache spec)
    (let [{:keys [family binding bound-value query] :as task}
          (benchmark-task spec)
          {:keys [size rows relations]}
          (when task (reference-for-task task))
          expected (case family
                     :tc (relation-row-count size rows binding bound-value)
                     :sg (relation-row-count size rows binding bound-value)
                     :join1 (relation-row-count size (get relations query)
                                                   binding bound-value)
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
;; Benchmark pass contract and child-runner CLI
;; ============================================================

(def default-run-options
  {:warmup 1 :iterations 1 :verify? true :quiet? false :output nil})

(defn- finish-runs
  [system spec runs verify?]
  (let [task          (benchmark-task spec)
        counts        (mapv :result-count runs)
        consistent?   (apply = counts)
        input-digests (mapv :input-digest runs)
        inputs-consistent? (apply = input-digests)
        expected      (when verify? (expected-result-count spec))
        oracle-ok?    (or (nil? expected)
                          (every? #(= expected %) counts))
        times         (mapv :time-ms runs)
        summary       (latency-summary times)
        single-pass?  (= 1 (count times))
        run-metadata  (select-keys (first runs)
                                   [:timing-scope :base-fact-count
                                    :engine-version :input-digest])
        correctness   (assoc
                        (cond
                          (not verify?)
                          {:status :skipped}

                          (some? expected)
                          {:status (if (and consistent?
                                            inputs-consistent?
                                            oracle-ok?)
                                     :passed :failed)
                           :oracle :independent-reference
                           :expected-count expected}

                          :else
                          {:status (if consistent? :consistent :failed)
                           :oracle :none})
                        :input-status (if inputs-consistent?
                                        :consistent :failed))]
    (if (and consistent? inputs-consistent? oracle-ok?)
      (merge
        {:system system
         :benchmark spec
         :task (dissoc task :spec)
         :status :ok
         :result-count (first counts)
         :time-ms (if single-pass? (first times) (:median summary))
         :reported-statistic (if single-pass? :single-measurement :median)
         :latency-ms summary
         :samples-ms times
         :correctness correctness}
        run-metadata)
      {:system system
       :benchmark spec
       :task (dissoc task :spec)
       :status :incorrect
       :result-counts counts
       :input-digests input-digests
       :expected-count expected
       :samples-ms times
       :correctness correctness})))

(defn- warmup-failures
  [benchmarks run-once passes]
  (loop [pass 0
         failures {}]
    (if (= pass passes)
      failures
      (recur
        (inc pass)
        (reduce
          (fn [current spec]
            (if (contains? current spec)
              current
              (let [result (run-once spec)]
                (if (= :ok (:status result))
                  current
                  (assoc current spec result)))))
          failures
          benchmarks)))))

(defn- measurement-runs
  [benchmarks run-once passes excluded]
  (loop [pass 0
         runs {}
         failures {}]
    (if (= pass passes)
      {:runs runs :failures failures}
      (let [[next-runs next-failures]
            (reduce
              (fn [[current-runs current-failures] spec]
                (if (or (contains? excluded spec)
                        (contains? current-failures spec))
                  [current-runs current-failures]
                  (let [result (run-once spec)]
                    (if (= :ok (:status result))
                      [(update current-runs spec (fnil conj []) result)
                       current-failures]
                      [current-runs (assoc current-failures spec result)]))))
              [runs failures]
              benchmarks)]
        (recur (inc pass) next-runs next-failures)))))

(defn run-benchmark-passes
  "Run complete warmup passes before complete measurement passes.

   The default protocol executes every selected task once as warmup and once as
   the retained measurement. More than one measurement pass is supported for
   diagnostic use and reports the median for compatibility with older data."
  [system benchmarks run-once {:keys [warmup iterations verify?]}]
  (let [warmup-errors (warmup-failures benchmarks run-once warmup)
        {:keys [runs failures]}
        (measurement-runs benchmarks run-once iterations warmup-errors)]
    (mapv
      (fn [spec]
        (if-let [failure (get warmup-errors spec)]
          (assoc failure
                 :system system
                 :benchmark spec
                 :phase :warmup
                 :samples-ms [])
          (if-let [failure (get failures spec)]
            (assoc failure
                   :system system
                   :benchmark spec
                   :phase :measurement
                   :samples-ms (mapv :time-ms (get runs spec)))
            (finish-runs system spec (get runs spec) verify?))))
      benchmarks)))

(defn run-repeated
  "Compatibility wrapper for running the pass protocol on one task."
  [system spec run-once opts]
  (first (run-benchmark-passes system [spec] run-once opts)))

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
            (require-benchmark-task arg)
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
   :processors (.availableProcessors (Runtime/getRuntime))
   :max-heap-bytes (.maxMemory (Runtime/getRuntime))
   :direct-linking (System/getProperty "clojure.compiler.direct-linking")})

(defn write-edn!
  [path value]
  (io/make-parents path)
  (spit path (with-out-str (pprint/pprint value))))

(defn child-usage
  [system]
  (str "OpenRuleBench " system " runner\n\n"
       "Options:\n"
       "  --warmup N       Complete warmup passes (default 1)\n"
       "  --iterations N   Complete measured passes (default 1)\n"
       "  --output PATH    Write an EDN report with raw measurements\n"
       "  --no-verify      Skip the independent answer-count oracle\n"
       "  --quiet          Suppress the console result row\n"
       "  --help           Show this help\n"))

(defn result-failure?
  [{:keys [status]}]
  (contains? #{:error :timeout :oom :incorrect} status))

(defn out-of-memory?
  "Return true when an engine wraps an OutOfMemoryError in an exception."
  [throwable]
  (loop [cause throwable]
    (when cause
      (or (instance? OutOfMemoryError cause)
          (boolean (some->> (.getMessage ^Throwable cause)
                            (re-find #"OutOfMemoryError|Java heap space")))
          (recur (.getCause ^Throwable cause))))))

(defn run-system-cli!
  [system default-benchmarks run-once args]
  (let [{:keys [benchmarks output quiet? help?] :as opts}
        (parse-run-args args default-benchmarks)]
    (if help?
      (do
        (println (child-usage system))
        {:exit-code 0 :help? true})
      (let [results (run-benchmark-passes system benchmarks run-once opts)
            report  {:format-version 2
                     :benchmark-suite :openrulebench
                     :contract :portable-tc-sg-join1-v1
                     :measurement-protocol
                     {:order :warmup-passes-then-measurement-passes
                      :reported-statistic
                      (if (= 1 (:iterations opts))
                        :single-measurement :median)}
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
