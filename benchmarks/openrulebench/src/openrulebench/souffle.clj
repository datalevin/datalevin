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
   [java.io File]
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

(def ^:private embedded-harness-source
  "#define __EMBEDDED_SOUFFLE__ 1
#include \"task.cpp\"
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << \"usage: task-bench FACT_DIR\\n\";
        return 2;
    }

    std::unique_ptr<souffle::SouffleProgram> program(
        souffle::ProgramFactory::newInstance(\"task\"));
    if (!program) {
        std::cerr << \"generated Souffle program was not registered\\n\";
        return 2;
    }

    program->loadAll(argv[1]);
    const auto start = std::chrono::steady_clock::now();
    program->runAll(\"\", \"\", false, true);
    const auto resultSize = program->getRelationSize(\"result\");
    const auto finish = std::chrono::steady_clock::now();
    if (!resultSize) {
        std::cerr << \"generated Souffle program has no result relation\\n\";
        return 2;
    }

    const std::chrono::duration<double, std::milli> elapsed = finish - start;
    std::cout << *resultSize << '\\t' << std::setprecision(17)
              << elapsed.count() << '\\n';
    return 0;
}
")

(def ^:private compiled-harnesses (atom {}))

(defn- delete-flat-dir!
  [dir]
  (doseq [f (or (seq (.listFiles (io/file dir))) [])]
    (io/delete-file f true))
  (io/delete-file dir true))

(defn cleanup-compiled-harnesses!
  []
  (let [entries (vals (swap! compiled-harnesses (constantly {})))]
    (doseq [{:keys [dir]} entries]
      (delete-flat-dir! dir))))

(defn- run-command!
  [description & args]
  (let [{:keys [exit out err] :as result} (apply sh/sh args)]
    (when-not (zero? exit)
      (throw (ex-info (str description " failed")
                      {:command args :exit exit :stdout out :stderr err})))
    result))

(defn- executable-on-path
  [executable]
  (some (fn [dir]
          (let [file (io/file dir executable)]
            (when (and (.isFile file) (.canExecute file)) file)))
        (str/split (or (System/getenv "PATH") "")
                   (re-pattern (java.util.regex.Pattern/quote
                                 File/pathSeparator)))))

(defn- souffle-include-dir
  []
  (let [executable (executable-on-path "souffle")
        installed  (when executable
                     (io/file (.getParentFile
                                (.getParentFile
                                  (.getCanonicalFile ^File executable)))
                              "include"))
        candidates [(some-> (System/getenv "SOUFFLE_INCLUDE_DIR") io/file)
                    installed
                    (io/file "/opt/homebrew/include")
                    (io/file "/usr/local/include")
                    (io/file "/usr/include")]
        ;; Some packaged installations place a second include root below
        ;; include/souffle. Prefer it when present so all generated and runtime
        ;; headers resolve from one consistent tree.
        roots      (mapcat #(when % [(io/file % "souffle") %]) candidates)]
    (some (fn [candidate]
            (when (and candidate
                       (.isFile (io/file candidate
                                         "souffle/CompiledSouffle.h")))
              (.getAbsolutePath ^File candidate)))
          roots)))

(defn- souffle-compiler-env
  []
  (let [include-dir (or (souffle-include-dir)
                        (throw (ex-info
                                 "Cannot locate Souffle C++ headers"
                                 {:environment "SOUFFLE_INCLUDE_DIR"})))
        current     (System/getenv "CPLUS_INCLUDE_PATH")]
    (assoc (into {} (System/getenv))
           "CPLUS_INCLUDE_PATH"
           (str include-dir
                (when-not (str/blank? current)
                  (str File/pathSeparator current))))))

(defn- run-command-with-env!
  [description env & args]
  (let [{:keys [exit out err] :as result}
        (apply sh/sh (concat args [:env env]))]
    (when-not (zero? exit)
      (throw (ex-info (str description " failed")
                      {:command args :exit exit :stdout out :stderr err})))
    result))

(defn- mach-o-rpaths
  [otool-output]
  (mapv second
        (re-seq #"(?m)^\s+path (.+) \(offset \d+\)$" otool-output)))

(defn- deduplicate-macos-rpaths!
  [binary]
  (when (= "Mac OS X" (System/getProperty "os.name"))
    (let [rpaths (mach-o-rpaths
                   (:out (run-command! "Mach-O inspection"
                                       "otool" "-l" binary)))]
      (doseq [[rpath occurrences] (frequencies rpaths)
              _ (range (dec occurrences))]
        ;; Souffle 2.5's Homebrew compiler configuration repeats a shared SDK
        ;; directory once per linked library. macOS 26 rejects that Mach-O at
        ;; load time, so retain one copy of each search path.
        (run-command! "Mach-O rpath cleanup"
                      "install_name_tool" "-delete_rpath" rpath binary)))))

(defn- souffle-generation-args
  [task cpp-file prog-file]
  (vec (concat ["souffle"]
               (when (not= :ff (:binding task)) ["-m" "result"])
               ["-g" cpp-file prog-file])))

(defn- compile-embedded-harness!
  [task program]
  (let [dir          (str "/tmp/openrulebench-souffle-compiled-"
                          (UUID/randomUUID))
        _            (io/make-parents (str dir "/dummy"))
        prog-file    (str dir "/task.dl")
        cpp-file     (str dir "/task.cpp")
        harness-file (str dir "/harness.cpp")
        binary       (str dir "/task-bench")]
    (try
      (spit prog-file program)
      (spit harness-file embedded-harness-source)
      (apply run-command! "Souffle C++ generation"
             (souffle-generation-args task cpp-file prog-file))
      (run-command-with-env! "Souffle harness compilation"
                             (souffle-compiler-env)
                             "souffle-compile.py" harness-file "-o" binary)
      (deduplicate-macos-rpaths! binary)
      {:binary binary :dir dir}
      (catch Throwable e
        (delete-flat-dir! dir)
        (throw e)))))

(defn- compiled-harness-for
  [task]
  (let [program (program-for-task task)]
    (or (get @compiled-harnesses program)
        (locking compiled-harnesses
          (or (get @compiled-harnesses program)
              (let [compiled (compile-embedded-harness! task program)]
                (swap! compiled-harnesses assoc program compiled)
                compiled))))))

(defn run-embedded-souffle-timed
  [binary fact-dir]
  (let [{:keys [exit out err]} (sh/sh binary fact-dir)]
    (when-not (zero? exit)
      (throw (ex-info "Souffle query harness failed"
                      {:binary binary :exit exit :stdout out :stderr err})))
    (let [[count-str time-str]
          (str/split (str/trim out) #"\s+")]
      (when-not (and count-str time-str)
        (throw (ex-info "Souffle query harness returned malformed output"
                        {:stdout out :stderr err})))
      {:result-count (parse-long count-str)
       :time-ms     (Double/parseDouble time-str)})))

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
        _prog-file (write-portable-files task task-data dir)]
    (try
      (let [{:keys [binary]} (compiled-harness-for task)
            {:keys [result-count time-ms]}
            (run-embedded-souffle-timed binary dir)]
        {:system "souffle"
         :benchmark spec
         :time-ms time-ms
         :result-count (or result-count 0)
         :base-fact-count (core/task-base-fact-count task task-data)
         :input-digest (core/task-data-digest task task-data)
         :engine-version (or @souffle-version "unknown")
         :timing-scope :query-and-materialization
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
                   (cleanup-compiled-harnesses!)
                   (shutdown-agents)))]
    (System/exit (:exit-code report))))
