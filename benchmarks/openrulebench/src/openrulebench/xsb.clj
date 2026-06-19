(ns openrulebench.xsb
  "XSB Prolog benchmarks for OpenRuleBench.

   Requires: XSB Prolog installed and 'xsb' in PATH.
   Install: http://xsb.sourceforge.net/"
  (:require
   [openrulebench.core :as core]
   [openrulebench.data :as data]
   [clojure.java.io :as io]
   [clojure.java.shell :as sh]
   [clojure.string :as str])
  (:import
   [java.util UUID]))

;; =============================================================================
;; Prolog Programs
;; =============================================================================

(def tc-program
  "% Transitive Closure
:- table tc/2.

tc(A, B) :- edge(A, B).
tc(A, B) :- edge(A, X), tc(X, B).

bench :- tc(_, _), fail.
bench.

count_tc(N) :- findall(1, tc(_, _), L), length(L, N).
")

(def sg-program
  "% Same Generation (OpenRuleBench spec)
:- table sg/2.

sg(X, X) :- parent(_, X).
sg(X, Y) :- parent(X, A), sg(A, B), parent(Y, B).

bench :- sg(_, _), fail.
bench.

count_sg(N) :- findall(1, sg(_, _), L), length(L, N).
")

;; =============================================================================
;; File Generation
;; =============================================================================

(defn write-tc-files
  "Write Prolog data and program files for TC benchmark."
  [edges dir]
  (let [data-file (str dir "/data.P")
        prog-file (str dir "/tc.P")]
    ;; Write edge facts
    (with-open [w (io/writer data-file)]
      (doseq [[a b] edges]
        (.write w (str "edge(" a ", " b ").\n"))))
    ;; Write program
    (spit prog-file (str ":- consult('" data-file "').\n" tc-program))
    prog-file))

(defn write-sg-files
  "Write Prolog data and program files for SG benchmark."
  [edges dir]
  (let [data-file (str dir "/data.P")
        prog-file (str dir "/sg.P")]
    ;; Write parent facts (edges are [parent child])
    (with-open [w (io/writer data-file)]
      (doseq [[p c] edges]
        (.write w (str "parent(" p ", " c ").\n"))))
    ;; Write program
    (spit prog-file (str ":- consult('" data-file "').\n" sg-program))
    prog-file))

;; =============================================================================
;; XSB Execution
;; =============================================================================

(defn run-xsb
  "Run XSB with a program file and goal."
  [prog-file goal]
  (let [result (sh/sh "xsb" "--nobanner" "--quietload"
                      "-e" (str "['" prog-file "'], " goal ", halt."))]
    (when (zero? (:exit result))
      (str/trim (:out result)))))

(defn run-xsb-timed
  "Run XSB benchmark and return count."
  [prog-file count-goal]
  (let [result (sh/sh "xsb" "--nobanner" "--quietload"
                      "-e" (str "['" prog-file "'], "
                                count-goal "(N), "
                                "write(N), nl, halt."))]
    (when (zero? (:exit result))
      (parse-long (str/trim (:out result))))))

;; =============================================================================
;; Benchmark Runners
;; =============================================================================

(defn run-tc-benchmark
  "Run TC benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [edges (data/generate-tc-instance (keyword instance-name))
        dir (str "/tmp/openrulebench-xsb-tc-" (UUID/randomUUID))
        _ (io/make-parents (str dir "/dummy"))
        prog-file (write-tc-files edges dir)
        _ (System/gc)
        start (core/now-ms)
        ;; First materialize (bench), then count
        _ (run-xsb prog-file "bench")
        result-count (run-xsb-timed prog-file "count_tc")
        time-ms (- (core/now-ms) start)]
    ;; Cleanup
    (doseq [f (.listFiles (io/file dir))]
      (io/delete-file f true))
    (io/delete-file dir true)
    {:system "xsb"
     :benchmark (str "tc:" instance-name)
     :time-ms time-ms
     :result-count (or result-count 0)
     :status (if result-count :ok :error)}))

(defn run-sg-benchmark
  "Run SG benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [edges (data/generate-sg-instance (keyword instance-name))
        dir (str "/tmp/openrulebench-xsb-sg-" (UUID/randomUUID))
        _ (io/make-parents (str dir "/dummy"))
        prog-file (write-sg-files edges dir)
        _ (System/gc)
        start (core/now-ms)
        ;; First materialize (bench), then count
        _ (run-xsb prog-file "bench")
        result-count (run-xsb-timed prog-file "count_sg")
        time-ms (- (core/now-ms) start)]
    ;; Cleanup
    (doseq [f (.listFiles (io/file dir))]
      (io/delete-file f true))
    (io/delete-file dir true)
    {:system "xsb"
     :benchmark (str "sg:" instance-name)
     :time-ms time-ms
     :result-count (or result-count 0)
     :status (if result-count :ok :error)}))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(def default-benchmarks
  ["tc:small" "tc:medium" "sg:small"])

(defn parse-benchmark [spec]
  (let [[bench-type instance] (str/split spec #":")]
    [bench-type instance]))

(defn run-benchmark [spec]
  (let [[bench-type instance] (parse-benchmark spec)]
    (try
      (case bench-type
        "tc" (run-tc-benchmark instance)
        "sg" (run-sg-benchmark instance)
        {:system "xsb" :benchmark spec :status :error})
      (catch Exception e
        (println "Error:" (.getMessage e))
        {:system "xsb" :benchmark spec :status :error}))))

(defn run-benchmarks [benchmark-specs]
  (doall (map run-benchmark benchmark-specs)))

(defn -main [& args]
  (let [benchmarks (if (seq args) args default-benchmarks)
        results (run-benchmarks benchmarks)]
    (core/print-row "xsb" results)))
