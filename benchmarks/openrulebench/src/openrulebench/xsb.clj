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

sg(X, Y) :- sib(X, Y).
sg(X, Y) :- par(X, Z), sg(Z, Z1), par(Y, Z1).

bench :- sg(_, _), fail.
bench.

count_sg(N) :- findall(1, sg(_, _), L), length(L, N).
")

(def join1-program
  "% JOIN1
:- table c1/2.
:- table b1/2.
:- table b2/2.
:- table a/2.

c1(X, Y) :- d1(X, Z), d2(Z, Y).
b2(X, Y) :- c3(X, Z), c4(Z, Y).
b1(X, Y) :- c1(X, Z), c2(Z, Y).
a(X, Y) :- b1(X, Z), b2(Z, Y).
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
  [{:keys [par sib]} dir]
  (let [data-file (str dir "/data.P")
        prog-file (str dir "/sg.P")]
    ;; Write par/sib facts
    (with-open [w (io/writer data-file)]
      (doseq [[a b] par]
        (.write w (str "par(" a ", " b ").\n")))
      (doseq [[a b] sib]
        (.write w (str "sib(" a ", " b ").\n"))))
    ;; Write program
    (spit prog-file (str ":- consult('" data-file "').\n" sg-program))
    prog-file))

(defn write-join1-files
  [relations dir]
  (let [data-file (str dir "/data.P")
        prog-file (str dir "/join1.P")]
    (with-open [w (io/writer data-file)]
      (doseq [relation [:d1 :d2 :c2 :c3 :c4]
              [a b] (get relations relation)]
        (.write w (str (name relation) "(" a ", " b ").\n"))))
    (spit prog-file (str ":- consult('" data-file "').\n" join1-program))
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

(defn run-xsb-count-goal
  "Run a complete XSB goal that binds N to the answer cardinality."
  [prog-file goal]
  (let [result (sh/sh "xsb" "--nobanner" "--quietload"
                      "-e" (str "['" prog-file "'], " goal ", "
                                "write(N), nl, halt."))]
    (when (zero? (:exit result))
      (parse-long (str/trim (:out result))))))

(def ^:private xsb-version
  (delay
    (try
      (let [{:keys [exit out err]} (sh/sh "xsb" "--version")]
        (when (zero? exit)
          (str/trim (if (str/blank? out) err out))))
      (catch Exception _ nil))))

(defn- answer-count-goal
  [{:keys [family query binding bound-value]}]
  (let [predicate (name (if (= family :join1) query family))]
    (case binding
      :ff (format "findall([X,Y], %s(X,Y), L), length(L,N)" predicate)
      :bf (format "findall(Y, %s(%d,Y), L), length(L,N)"
                  predicate bound-value)
      :fb (format "findall(X, %s(X,%d), L), length(L,N)"
                  predicate bound-value))))

;; =============================================================================
;; Benchmark Runners
;; =============================================================================

(defn run-portable-benchmark
  [{:keys [family spec] :as task}]
  (let [task-data (core/generate-task-data task)
        dir       (str "/tmp/openrulebench-xsb-" (name family) "-"
                       (UUID/randomUUID))
        _         (io/make-parents (str dir "/dummy"))
        prog-file (case family
                    :tc (write-tc-files task-data dir)
                    :sg (write-sg-files task-data dir)
                    :join1 (write-join1-files task-data dir))]
    (try
      (let [start        (core/now-ms)
            result-count (run-xsb-count-goal prog-file
                                             (answer-count-goal task))
            time-ms      (- (core/now-ms) start)]
        {:system "xsb"
         :benchmark spec
         :time-ms time-ms
         :result-count (or result-count 0)
         :base-fact-count (core/task-base-fact-count task task-data)
         :input-digest (core/task-data-digest task task-data)
         :engine-version (or @xsb-version "unknown")
         :timing-scope :external-process-load-evaluate-materialize
         :status (if result-count :ok :error)})
      (finally
        (doseq [f (or (seq (.listFiles (io/file dir))) [])]
          (io/delete-file f true))
        (io/delete-file dir true)))))

(defn run-tc-benchmark
  "Run TC benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [edges (data/generate-tc-instance (keyword instance-name))
        dir (str "/tmp/openrulebench-xsb-tc-" (UUID/randomUUID))
        _ (io/make-parents (str dir "/dummy"))
        prog-file (write-tc-files edges dir)]
    (try
      (System/gc)
      (let [start        (core/now-ms)
            ;; The count goal materializes the table and consumes all answers
            ;; in one XSB process; a separate pre-run would not warm this process.
            result-count (run-xsb-timed prog-file "count_tc")
            time-ms      (- (core/now-ms) start)]
        {:system "xsb"
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
        dir (str "/tmp/openrulebench-xsb-sg-" (UUID/randomUUID))
        _ (io/make-parents (str dir "/dummy"))
        prog-file (write-sg-files relations dir)]
    (try
      (System/gc)
      (let [start        (core/now-ms)
            result-count (run-xsb-timed prog-file "count_sg")
            time-ms      (- (core/now-ms) start)]
        {:system "xsb"
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
      {:system "xsb" :benchmark spec :status :error
       :error (.getMessage e)})))

(defn run-benchmarks [benchmark-specs]
  (doall (map run-benchmark benchmark-specs)))

(defn -main [& args]
  (let [report (try
                 (core/run-system-cli! "xsb" default-benchmarks
                                       run-benchmark args)
                 (finally
                   (shutdown-agents)))]
    (System/exit (:exit-code report))))
