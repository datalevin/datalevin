(ns openrulebench.core
  "Core benchmarking utilities for OpenRuleBench.
   Simple single-run timing with wall clock time."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

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
  (doseq [{:keys [time-ms status result-count]} results]
    (case status
      :ok      (print (round time-ms) "\t")
      :timeout (print "T/O\t")
      :oom     (print "OOM\t")
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
