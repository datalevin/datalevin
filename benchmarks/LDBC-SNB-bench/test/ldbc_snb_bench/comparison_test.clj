(ns ldbc-snb-bench.comparison-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ldbc-snb-bench.comparison :as comparison]))

(defn- report
  [system medians digests]
  {:format-version 1
   :benchmark-suite :ldbc-snb-interactive-v1-read-latency
   :benchmark-system system
   :official-ldbc-result false
   :exit-code 0
   :host {:processors 12
          :process-id 100
          :jvm-instance-id (str system "-jvm")
          :java "21"
          :vm "OpenJDK"
          :clojure-direct-linking true
          :os "Mac OS X"
          :os-version "26"
          :arch "aarch64"
          :max-heap-bytes 1000}
   :dataset {:scale-factor "1"}
   :configuration {:warmup 1
                   :iterations 5
                   :parameter-count 1
                   :seed 42
                   :scale-factor "1"
                   :verify? true
                   :query-cache? false
                   :query-names []}
   :timing-boundary {:included [:query-execution :result-realization]
                     :excluded [:database-open]}
   :parameter-schedule {:sha256 "same-schedule"}
   :summaries
   (mapv (fn [[query median]]
           {:query query
            :sample-count 5
            :status :ok
            :result-counts [1]
            :latency-ms {:median median :p95 (* median 1.1)}})
         medians)
   :results
   (mapv (fn [[query digest]]
           {:name query
            :selection-index 0
            :source-index 0
            :status :ok
            :result-count 1
            :result-sha256 digest})
         digests)})

(deftest comparable-reports-test
  (let [left (report "Left" [["IC1" 10.0] ["IS1" 2.0]]
                     [["IC1" "same"] ["IS1" "left"]])
        right (report "Right" [["IC1" 5.0] ["IS1" 4.0]]
                      [["IC1" "same"] ["IS1" "right"]])
        result (comparison/compare-reports left right)
        [ic1 is1] (:rows result)]
    (is (= 0.5 (:right-over-left-median-ratio ic1)))
    (is (= "Right" (:lower-median-system ic1)))
    (is (= 2.0 (:lower-median-factor ic1)))
    (is (= 2.0 (:right-over-left-median-ratio is1)))
    (is (= "Left" (:lower-median-system is1)))
    (is (= 1 (get-in result
                     [:result-validation :exact-result-digest-match-count])))
    (is (= ["IC1"]
           (get-in result
                   [:result-validation :exact-result-digest-matches])))
    (is (= 0.75
           (get-in result
                   [:aggregates :all :right-over-left-sum-ratio])))
    (is (str/includes? (comparison/markdown result)
                       "| IC1 | 10.000 | 5.000 | 0.500x | Right 2.000x |"))))

(deftest one-sample-markdown-test
  (let [one-sample
        (fn [value]
          (-> value
              (assoc-in [:configuration :warmup] 0)
              (assoc-in [:configuration :iterations] 1)
              (assoc-in [:configuration :run-role] :measurement)
              (update :summaries
                      (fn [summaries]
                        (mapv #(assoc % :sample-count 1) summaries)))))
        left (one-sample
               (report "Left" [["IC1" 10.0]] [["IC1" "same"]]))
        right (one-sample
                (report "Right" [["IC1" 5.0]] [["IC1" "different"]]))
        result (comparison/compare-reports left right)
        markdown (comparison/markdown result)
        csv-file (java.io.File/createTempFile "ldbc-comparison-" ".csv")]
    (is (str/includes? markdown "Left measured (ms)"))
    (is (str/includes? markdown "sum of measured times (ms)"))
    (is (not (str/includes? markdown "median (ms)")))
    (try
      (comparison/write-csv! (.getPath csv-file) result)
      (let [csv (slurp csv-file)]
        (is (str/includes? csv "Sample Count"))
        (is (str/includes? csv "Left Measured (ms)"))
        (is (str/includes? csv ",false\n")))
      (finally
        (.delete csv-file)))))

(deftest incompatible-reports-test
  (let [left (report "Left" [["IC1" 10.0]] [["IC1" "same"]])
        right (report "Right" [["IC1" 5.0]] [["IC1" "same"]])]
    (testing "schedule mismatches are rejected"
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"parameter-schedule-sha256 differs"
            (comparison/compare-reports
              left (assoc-in right [:parameter-schedule :sha256] "other")))))
    (testing "failed runs are rejected"
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"did not complete successfully"
            (comparison/compare-reports left (assoc right :exit-code 1)))))
    (testing "compiler-mode mismatches are rejected"
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"host differs"
            (comparison/compare-reports
              left
              (assoc-in right [:host :clojure-direct-linking] false)))))))

(deftest independent-pass-verification-test
  (let [warmup (assoc-in
                 (assoc-in
                   (report "Datalevin" [["IC1" 12.0]] [["IC1" "same"]])
                   [:host :jvm-instance-id]
                   "warmup-jvm")
                 [:configuration :run-role]
                 :warmup)
        measurement
        (-> (report "Datalevin" [["IC1" 10.0]] [["IC1" "same"]])
            (assoc-in [:host :process-id] 200)
            (assoc-in [:host :jvm-instance-id] "measurement-jvm")
            (assoc-in [:configuration :run-role] :measurement))]
    (is (= {:system "Datalevin"
            :warmup-report-path nil
            :measurement-report-path nil
            :parameter-schedule-sha256 "same-schedule"
            :result-count 1
            :independent-client-processes? true
            :processes
            {:warmup {:process-id 100
                      :jvm-instance-id "warmup-jvm"}
             :measurement {:process-id 200
                           :jvm-instance-id "measurement-jvm"}}
            :exact-result-digests-match? true}
           (comparison/verify-independent-pass warmup measurement)))
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"same JVM process"
          (comparison/verify-independent-pass
            warmup
            (assoc-in measurement [:host :jvm-instance-id] "warmup-jvm"))))
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"results differ"
          (comparison/verify-independent-pass
            warmup
            (assoc-in measurement [:results 0 :result-sha256] "changed"))))
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"database-indexes differs"
          (comparison/verify-independent-pass
            (assoc-in warmup [:neo4j :server :indexes] [{:name "id"}])
            (assoc-in measurement [:neo4j :server :indexes] []))))))
