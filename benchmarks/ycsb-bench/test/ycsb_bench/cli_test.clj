(ns ycsb-bench.cli-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]]
            [datalevin-bench.host :as host]
            [ycsb-bench.core :as core]
            [ycsb-bench.runner :as runner])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.time Instant]))

(use-fixtures :each
  (fn [f]
    ;; CLI tests share the test JVM: keep its executors alive and leave the
    ;; host's processes alone. Parsing, benchmarking, and report I/O stay real.
    (with-redefs [shutdown-agents (fn [])
                  host/pause! (constantly [])
                  host/resume! (fn [_])]
      (f))))

(def small-args
  ["--api" "DATALOG" "--mode" "embedded" "--workload" "c"
   "--records" "4" "--ops" "5" "--warmup" "0" "--threads" "2"
   "--field-count" "2" "--field-length" "4" "--batch-size" "2"
   "--seed" "23"])

(defn- with-report-path [f]
  (let [directory (Files/createTempDirectory "datalevin-ycsb-cli-"
                                              (make-array FileAttribute 0))
        path (.resolve directory "report.edn")]
    (try
      (f (.toFile path))
      (finally
        (Files/deleteIfExists path)
        (Files/delete directory)))))

(defn- check-report! [report systems]
  (is (= {:format-version 3 :benchmark :datalevin-ycsb-style
          :measurement-model :closed-loop}
         (select-keys report [:format-version :benchmark :measurement-model])))
  (is (string? (:datalevin-version report)))
  (is (instance? Instant (Instant/parse (:started-at report))))
  (is (pos? (get-in report [:environment :processors])))
  (is (= systems (mapv #(get-in % [:configuration :system]) (:results report))))
  (doseq [result (:results report)]
    (is (= {:api :datalog :mode :embedded :workload :c :records 4 :ops 5
            :warmup 0 :threads 2 :pool-size 2 :field-count 2 :field-length 4
            :seed 23}
           (select-keys (:configuration result)
                        [:api :mode :workload :records :ops :warmup :threads
                         :pool-size :field-count :field-length :seed])))
    (is (= 0 (get-in result [:warmup :operations])))
    (is (= 5 (get-in result [:measured :by-operation :read :count])))
    (is (number? (get-in result [:measured :latency-us :p99])))
    (is (= {:status :passed :records 4 :all-records-checked? true
            :scope :structure :value-checks :not-performed
            :character-checks :post-measurement}
           (:validation result)))
    (is (not-any? #(contains? (:configuration result) %)
                  [:output :help :pg-url :pg-user]))))

(deftest help-and-invalid-arguments-test
  (with-redefs [runner/run-case! (fn [_] (throw (AssertionError. "Unexpected benchmark run")))
                host/pause! (fn [] (throw (AssertionError. "Unexpected host control")))]
    (let [output (with-out-str (core/-main "--help"))]
      (is (str/includes? output "YCSB-style Datalevin benchmark"))
      (is (str/includes? output "--system"))
      (is (str/includes? output "--sql-indexes"))
      (is (str/includes? output "--output")))
    (with-report-path
      (fn [file]
        (doseq [args [["--unknown-option"] ["unexpected-argument"]
                      ["--ops"] ["--ops" "not-a-number"] ["--ops" "0"]
                      ["--system" "postgres" "--mode" "embedded"]]]
          (is (thrown? clojure.lang.ExceptionInfo
                       (apply core/-main (concat ["--output" (str file)] args)))
              (pr-str args))
          (is (not (.exists file))))))))

(deftest cli-file-report-test
  (with-report-path
    (fn [file]
      (let [output (with-out-str
                     (apply core/-main
                            (concat small-args
                                    ["--system" "all" "--output" (str file)
                                     "--pg-url" "unused-secret-url"
                                     "--pg-user" "unused-secret-user"])))
            serialized (slurp file)]
        (check-report! (edn/read-string serialized) [:datalevin :sqlite])
        (is (str/includes? output "datalevin datalog embedded C:"))
        (is (str/includes? output "sqlite datalog embedded C:"))
        (is (str/includes? output (str "Report: " file)))
        (is (not (str/includes? serialized "unused-secret")))))))

(deftest cli-stdout-report-test
  (let [output (with-out-str
                 (apply core/-main (concat small-args ["--system" "sqlite"])))
        ;; Status lines precede the EDN report on stdout.
        report (edn/read-string (subs output (str/index-of output "{")))]
    (check-report! report [:sqlite])
    (is (str/includes? output "sqlite datalog embedded C:"))
    (is (not (str/includes? output "Report:")))))

(deftest cli-value-audit-report-test
  (let [output (with-out-str
                 (apply core/-main (concat small-args ["--system" "sqlite" "--value-audit"])))
        report (edn/read-string (subs output (str/index-of output "{")))
        result (first (:results report))]
    (is (true? (get-in result [:configuration :value-audit?])))
    (is (= :structure-and-observed-values (get-in result [:validation :scope])))
    (is (= :passed (get-in result [:validation :value-checks :status])))
    (is (= 5 (get-in result [:validation :value-checks :point-reads])))
    (is (str/includes? output "timings include recording"))))

(deftest cli-sql-index-conditions-report-test
  (with-report-path
    (fn [file]
      (let [output (with-out-str
                     (apply core/-main
                            (concat small-args
                                    ["--system" "sqlite" "--sql-indexes" "both"
                                     "--output" (str file)])))
            report (edn/read-string (slurp file))
            results (:results report)]
        (check-report! report [:sqlite :sqlite])
        (is (= [:none :all] (mapv #(get-in % [:configuration :sql-indexes]) results)))
        (is (= [:none :all] (mapv #(get-in % [:storage :configuration :sql-indexes]) results)))
        (is (= [0 2] (mapv #(count (get-in % [:storage :configuration :secondary-indexes])) results)))
        (is (= #{:none :all} (set (map #(get-in % [:configuration :sql-indexes]) (:summary report)))))
        (is (str/includes? output "SQL indexes none"))
        (is (str/includes? output "SQL indexes all"))))))

(deftest failed-case-does-not-publish-report-test
  (doseq [existing? [false true]]
    (with-report-path
      (fn [file]
        (let [previous "existing report\n"
              failure (ex-info "Injected case validation failure" {})
              calls (atom [])]
          (when existing? (spit file previous))
          (with-redefs [runner/run-case!
                        (fn [{:keys [system]}]
                          (swap! calls conj system)
                          (if (= system :sqlite)
                            (throw failure)
                            {:measured {:ops-per-second 1.0 :latency-us {:p99 1.0}}
                             :validation {:records 4}}))]
            (is (identical? failure
                            (try
                              (with-out-str
                                (apply core/-main (concat small-args
                                                         ["--system" "all"
                                                          "--output" (str file)])))
                              (catch Exception e e)))))
          (is (= [:datalevin :sqlite] @calls))
          (if existing?
            (is (= previous (slurp file)))
            (is (not (.exists file)))))))))

(deftest timed-repeated-cli-report-test
  (with-report-path
    (fn [file]
      (with-out-str
        (apply core/-main
               (concat small-args
                       ["--system" "sqlite" "--client-counts" "1,2"
                        "--repetitions" "2" "--warmup-ms" "0" "--measurement-ms" "20"
                        "--server-workers" "12" "--output" (str file)])))
      (let [report (edn/read-string (slurp file))
            results (:results report)]
        (is (= 4 (count results)))
        (is (= [1 2 2 1] (mapv #(get-in % [:configuration :threads]) results)))
        (is (= [1 1 2 2] (mapv #(get-in % [:configuration :trial]) results)))
        (is (every? #(>= (get-in % [:measured :seconds]) 0.02) results))
        (is (every? #(= 0 (get-in % [:warmup :operations])) results))
        (is (= [2 2] (mapv :trials (:summary report))))))))
