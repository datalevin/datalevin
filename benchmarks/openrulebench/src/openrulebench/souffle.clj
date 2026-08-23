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

(def join1-program
  ".decl d1(a: number, b: number)
.input d1
.decl d2(a: number, b: number)
.input d2
.decl c2(a: number, b: number)
.input c2
.decl c3(a: number, b: number)
.input c3
.decl c4(a: number, b: number)
.input c4

.decl c1(x: number, y: number)
.decl b1(x: number, y: number)
.decl b2(x: number, y: number)
.decl a(x: number, y: number)

c1(x, y) :- d1(x, z), d2(z, y).
b2(x, y) :- c3(x, z), c4(z, y).
b1(x, y) :- c1(x, z), c2(z, y).
a(x, y) :- b1(x, z), b2(z, y).
")

(defn- result-program
  [predicate binding bound]
  (case binding
    :ff (format ".decl result(x: number, y: number)\n.output result\n\nresult(x, y) :- %s(x, y).\n"
                predicate)
    :bf (format ".decl result(y: number)\n.output result\n\nresult(y) :- %s(%d, y).\n"
                predicate bound)
    :fb (format ".decl result(x: number)\n.output result\n\nresult(x) :- %s(x, %d).\n"
                predicate bound)))

(defn program-for-task
  [{:keys [family query binding bound-value]}]
  (let [base (case family
               :tc (str/replace tc-program ".output tc\n" "")
               :sg (str/replace sg-program ".output sg\n" "")
               :join1 join1-program)
        predicate (name (if (= family :join1) query family))]
    (str base "\n" (result-program predicate binding bound-value))))

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

(defn write-portable-files
  [{:keys [family] :as task} task-data dir]
  (case family
    :tc (with-open [w (io/writer (str dir "/edge.facts"))]
          (doseq [[a b] task-data]
            (.write w (str a "\t" b "\n"))))
    :sg (do
          (with-open [w (io/writer (str dir "/par.facts"))]
            (doseq [[a b] (:par task-data)]
              (.write w (str a "\t" b "\n"))))
          (with-open [w (io/writer (str dir "/sib.facts"))]
            (doseq [[a b] (:sib task-data)]
              (.write w (str a "\t" b "\n")))))
    :join1 (doseq [relation [:d1 :d2 :c2 :c3 :c4]]
             (with-open [w (io/writer (str dir "/" (name relation)
                                           ".facts"))]
               (doseq [[a b] (get task-data relation)]
                 (.write w (str a "\t" b "\n"))))))
  (let [prog-file (str dir "/task.dl")]
    (spit prog-file (program-for-task task))
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

(def ^:private souffle-version
  (delay
    (try
      (let [{:keys [exit out err]} (sh/sh "souffle" "--version")]
        (when (zero? exit)
          (str/trim (if (str/blank? out) err out))))
      (catch Exception _ nil))))

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

(defn run-portable-benchmark
  [{:keys [family spec] :as task}]
  (let [task-data (core/generate-task-data task)
        dir       (str "/tmp/openrulebench-souffle-" (name family) "-"
                       (UUID/randomUUID))
        _         (io/make-parents (str dir "/dummy"))
        prog-file (write-portable-files task task-data dir)]
    (try
      (let [[output-dir time-ms] (core/time-once (run-souffle prog-file dir))
            result-count (when output-dir (count-output output-dir "result"))]
        {:system "souffle"
         :benchmark spec
         :time-ms time-ms
         :result-count (or result-count 0)
         :base-fact-count (core/task-base-fact-count task task-data)
         :input-digest (core/task-data-digest task task-data)
         :engine-version (or @souffle-version "unknown")
         :timing-scope :external-process-compile-load-evaluate-materialize
         :status (if result-count :ok :error)})
      (finally
        (doseq [f (or (seq (.listFiles (io/file dir))) [])]
          (io/delete-file f true))
        (io/delete-file dir true)))))

(defn run-tc-benchmark
  "Run TC benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [edges (data/generate-tc-instance (keyword instance-name))
        dir (str "/tmp/openrulebench-souffle-tc-" (UUID/randomUUID))
        _ (io/make-parents (str dir "/dummy"))
        prog-file (write-tc-files edges dir)]
    (try
      (System/gc)
      (let [[output-dir time-ms] (core/time-once (run-souffle prog-file dir))
            result-count (when output-dir (count-output output-dir "tc"))]
        {:system "souffle"
         :benchmark (str "tc:" instance-name)
         :time-ms time-ms
         :result-count (or result-count 0)
         :status (if result-count :ok :error)})
      (finally
        (doseq [f (.listFiles (io/file dir))]
          (io/delete-file f true))
        (io/delete-file dir true)))))

(defn run-sg-benchmark
  "Run SG benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [relations (data/generate-sg-instance (keyword instance-name))
        dir (str "/tmp/openrulebench-souffle-sg-" (UUID/randomUUID))
        _ (io/make-parents (str dir "/dummy"))
        prog-file (write-sg-files relations dir)]
    (try
      (System/gc)
      (let [[output-dir time-ms] (core/time-once (run-souffle prog-file dir))
            result-count (when output-dir (count-output output-dir "sg"))]
        {:system "souffle"
         :benchmark (str "sg:" instance-name)
         :time-ms time-ms
         :result-count (or result-count 0)
         :status (if result-count :ok :error)})
      (finally
        (doseq [f (.listFiles (io/file dir))]
          (io/delete-file f true))
        (io/delete-file dir true)))))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(def default-benchmarks
  ["tc:50k-cyclic-ff" "sg:6k-cyclic-ff"])

(defn parse-benchmark [spec]
  (core/parse-benchmark spec))

(defn run-benchmark [spec]
  (try
    (run-portable-benchmark (core/require-benchmark-task spec))
    (catch Exception e
      (println "Error:" (.getMessage e))
      {:system "souffle" :benchmark spec :status :error
       :error (.getMessage e)})))

(defn run-benchmarks [benchmark-specs]
  (doall (map run-benchmark benchmark-specs)))

(defn -main [& args]
  (let [report (try
                 (core/run-system-cli! "souffle" default-benchmarks
                                       run-benchmark args)
                 (finally
                   (shutdown-agents)))]
    (System/exit (:exit-code report))))
