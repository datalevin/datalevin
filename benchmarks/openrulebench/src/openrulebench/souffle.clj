(ns openrulebench.souffle
  "Souffle benchmarks for OpenRuleBench.

   Requires: Souffle installed and 'souffle' in PATH.
   Install: https://souffle-lang.github.io/install"
  (:require
   [openrulebench.core :as core]
   [openrulebench.data :as data]
   [clojure.java.io :as io]
   [clojure.java.shell :as sh]
   [clojure.string :as str])
  (:import
   [java.util UUID]))

;; =============================================================================
;; Souffle Programs (Datalog)
;; =============================================================================

(def tc-program
  "// Transitive Closure
.decl edge(a: number, b: number)
.input edge

.decl tc(a: number, b: number)
.output tc

tc(a, b) :- edge(a, b).
tc(a, b) :- edge(a, x), tc(x, b).
")

(def sg-program
  "// Same Generation (OpenRuleBench spec)
.decl par(a: number, b: number)
.input par

.decl sib(a: number, b: number)
.input sib

.decl sg(x: number, y: number)
.output sg

sg(x, y) :- sib(x, y).
sg(x, y) :- par(x, z), sg(z, z1), par(y, z1).
")

;; =============================================================================
;; File Generation
;; =============================================================================

(defn write-tc-files
  "Write Souffle data and program files for TC benchmark."
  [edges dir]
  (let [data-file (str dir "/edge.facts")
        prog-file (str dir "/tc.dl")]
    ;; Write edge facts (tab-separated)
    (with-open [w (io/writer data-file)]
      (doseq [[a b] edges]
        (.write w (str a "\t" b "\n"))))
    ;; Write program
    (spit prog-file tc-program)
    prog-file))

(defn write-sg-files
  "Write Souffle data and program files for SG benchmark."
  [{:keys [par sib]} dir]
  (let [par-file (str dir "/par.facts")
        sib-file (str dir "/sib.facts")
        prog-file (str dir "/sg.dl")]
    ;; Write par/sib facts (tab-separated)
    (with-open [w (io/writer par-file)]
      (doseq [[a b] par]
        (.write w (str a "\t" b "\n"))))
    (with-open [w (io/writer sib-file)]
      (doseq [[a b] sib]
        (.write w (str a "\t" b "\n"))))
    ;; Write program
    (spit prog-file sg-program)
    prog-file))

;; =============================================================================
;; Souffle Execution
;; =============================================================================

(defn run-souffle
  "Run Souffle with a program file. Returns output directory."
  [prog-file dir]
  (let [result (sh/sh "souffle" "-F" dir "-D" dir prog-file)]
    (when (zero? (:exit result))
      dir)))

(defn count-output
  "Count lines in output file."
  [dir output-name]
  (let [output-file (io/file dir (str output-name ".csv"))]
    (when (.exists output-file)
      (with-open [rdr (io/reader output-file)]
        (count (line-seq rdr))))))

;; =============================================================================
;; Benchmark Runners
;; =============================================================================

(defn run-tc-benchmark
  "Run TC benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [edges (data/generate-tc-instance (keyword instance-name))
        dir (str "/tmp/openrulebench-souffle-tc-" (UUID/randomUUID))
        _ (io/make-parents (str dir "/dummy"))
        prog-file (write-tc-files edges dir)
        _ (System/gc)
        [output-dir time-ms] (core/time-once (run-souffle prog-file dir))
        result-count (when output-dir (count-output output-dir "tc"))]
    ;; Cleanup
    (doseq [f (.listFiles (io/file dir))]
      (io/delete-file f true))
    (io/delete-file dir true)
    {:system "souffle"
     :benchmark (str "tc:" instance-name)
     :time-ms time-ms
     :result-count (or result-count 0)
     :status (if result-count :ok :error)}))

(defn run-sg-benchmark
  "Run SG benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [relations (data/generate-sg-instance (keyword instance-name))
        dir (str "/tmp/openrulebench-souffle-sg-" (UUID/randomUUID))
        _ (io/make-parents (str dir "/dummy"))
        prog-file (write-sg-files relations dir)
        _ (System/gc)
        [output-dir time-ms] (core/time-once (run-souffle prog-file dir))
        result-count (when output-dir (count-output output-dir "sg"))]
    ;; Cleanup
    (doseq [f (.listFiles (io/file dir))]
      (io/delete-file f true))
    (io/delete-file dir true)
    {:system "souffle"
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
        {:system "souffle" :benchmark spec :status :error})
      (catch Exception e
        (println "Error:" (.getMessage e))
        {:system "souffle" :benchmark spec :status :error}))))

(defn run-benchmarks [benchmark-specs]
  (doall (map run-benchmark benchmark-specs)))

(defn -main [& args]
  (let [benchmarks (if (seq args) args default-benchmarks)
        results (run-benchmarks benchmarks)]
    (core/print-row "souffle" results)))
