(ns datalevin-bench.evaluation-analysis-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin-bench.evaluation-analysis :as analysis])
  (:import
   [java.nio.charset StandardCharsets]
   [java.util Base64]))

(def rows
  [{:query "1a" :sample-seed 1 :mode :production-t0 :base-mode :full
    :policy :production :tail-weight 0.0 :baseline? true :status :ok
    :execution-ms 10.0 :planning-ms 1.0 :plan-hash 10}
   {:query "1a" :sample-seed 2 :mode :production-t0 :base-mode :full
    :policy :production :tail-weight 0.0 :baseline? true :status :ok
    :execution-ms 20.0 :planning-ms 1.5 :plan-hash 10}
   {:query "1a" :sample-seed 1 :mode :raw-t10 :base-mode :raw-sampling
    :policy :raw :tail-weight 10.0 :baseline? false :status :ok
    :execution-ms 150.0 :planning-ms 0.5 :plan-hash 20}
   {:query "1a" :sample-seed 2 :mode :raw-t10 :base-mode :raw-sampling
    :policy :raw :tail-weight 10.0 :baseline? false :status :timeout
    :planning-ms 0.5}
   {:query "2a" :sample-seed 1 :mode :production-t0 :base-mode :full
    :policy :production :tail-weight 0.0 :baseline? true :status :ok
    :execution-ms 5.0 :planning-ms 1.0 :plan-hash 30}
   {:query "2a" :sample-seed 1 :mode :raw-t10 :base-mode :raw-sampling
    :policy :raw :tail-weight 10.0 :baseline? false :status :ok
    :execution-ms 5.0 :planning-ms 1.0 :plan-hash 30}])

(deftest tail-summary-retains-timeouts
  (let [summary (analysis/summarize rows 30000)
        raw     (get-in summary [:conditions :raw-t10])]
    (is (= :production-t0 (:baseline-condition summary)))
    (is (= 3 (:trials raw)))
    (is (= 1 (:timeouts raw)))
    (is (= 2 (:catastrophes raw)))
    (is (= 150.0 (:max (:execution-ms raw)))
        "successful execution distribution does not impute timeout")
    (is (= 2000.0 (:max (:slowdown raw))))
    (is (= 30155.0 (:capped-execution-ms raw)))))

(deftest plan-disagreement-census
  (is (= [{:query "1a"
           :sample-seed 1
           :plans (sorted-map :production-t0 10 :raw-t10 20)}]
         (analysis/plan-disagreements rows))))

(deftest diagnostic-replay-enriches-plan-without-changing-measured-time
  (let [timing [{:run 0 :sample-seed 11 :mode :full :query "1a"
                 :status :ok :execution-ms 12.5 :timing-method :plain-query}]
        diagnostics
        [{:run 0 :sample-seed 11 :mode :full :query "1a"
          :status :ok :planning-ms 2.0 :plan-hash 42
          :timing-method :explain :plan-only? true}]
        [row] (analysis/merge-diagnostics timing diagnostics)]
    (is (= 12.5 (:execution-ms row)))
    (is (= :plain-query (:timing-method row)))
    (is (= 2.0 (:planning-ms row)))
    (is (= 42 (:plan-hash row)))
    (is (= :ok (:diagnostic-status row)))))

(deftest causal-summary-links-tail-events-to-plan-changes
  (let [summary (analysis/summarize rows 30000)
        raw     (get-in summary [:causal-comparisons :raw-t10])
        pairwise (first (:pairwise-plan-comparisons summary))]
    (is (= 2 (:plan-comparable-pairs raw)))
    (is (= 1 (:plan-changed-pairs raw)))
    (is (= 2 (:catastrophes raw)))
    (is (= 1 (:plan-different-catastrophes raw)))
    (is (= 1 (:timeout-catastrophes raw)))
    (is (= [:production-t0 :raw-t10] (:conditions pairwise)))
    (is (= 2 (:comparable-pairs pairwise)))
    (is (= 1 (:different-pairs pairwise)))
    (is (= 1 (:queries-with-any-difference pairwise)))))

(deftest worker-rewarm-audit-compares-against-other-seeds
  (let [rewarm-rows
        [{:query "1a" :sample-seed 1 :mode :full :baseline? true
          :status :ok :execution-ms 12.0 :worker-rewarmed? true}
         {:query "1a" :sample-seed 2 :mode :full :baseline? true
          :status :ok :execution-ms 10.0}
         {:query "1a" :sample-seed 3 :mode :full :baseline? true
          :status :ok :execution-ms 14.0}
         {:query "2a" :sample-seed 1 :mode :full :baseline? true
          :status :timeout :worker-rewarmed? true}]
        audit (:worker-rewarm (analysis/summarize rewarm-rows 30000))]
    (is (= 2 (:rows audit)))
    (is (= 1 (:successful-comparisons audit)))
    (is (= 1.0 (get-in audit
                       [:relative-to-other-seed-median :p50])))
    (is (zero? (:over-2x audit)))))

(deftest legacy-plan-decisions-recover-structural-hash
  (let [encode (fn [value]
                 (.encodeToString
                   (Base64/getEncoder)
                   (.getBytes (pr-str value) StandardCharsets/UTF_8)))
        first-plan [{:steps ["a" "b"] :size 10.0 :cost 2.0}]
        same-plan  [{:steps ["a" "b"] :size 100.0 :cost 20.0}]
        first-gensym [{:steps ["Initialize [?bound123] by x"]}]
        same-gensym  [{:steps ["Initialize [?bound987] by x"]}]
        first-blank  [{:steps ["Initialize [?blank36025] by x"]}]
        same-blank   [{:steps ["Initialize [?blank36030] by x"]}]]
    (is (= (#'analysis/structural-plan-hash (encode first-plan))
           (#'analysis/structural-plan-hash (encode same-plan))))
    (is (= (#'analysis/structural-plan-hash (encode first-gensym))
           (#'analysis/structural-plan-hash (encode same-gensym))))
    (is (= (#'analysis/structural-plan-hash (encode first-blank))
           (#'analysis/structural-plan-hash (encode same-blank))))))

(deftest hierarchical-bootstrap-is-deterministic
  (let [options {:bootstrap-samples 40 :bootstrap-seed 99}
        first   (:bootstrap (analysis/summarize rows 30000 options))
        replay  (:bootstrap (analysis/summarize rows 30000 options))]
    (is (= first replay))
    (is (= :hierarchical-paired-query-then-seed-bootstrap
           (:method first)))
    (is (= 40 (:samples first)))
    (is (vector?
          (get-in first
                  [:contrasts-versus-baseline :raw-t10
                   :catastrophe-rate :ci95])))
    (is (vector?
          (get-in first
                  [:pairwise-contrasts
                   [:production-t0 :raw-t10]
                   :catastrophe-rate :ci95])))))

(deftest paired-runtime-ratios-use-the-matched-baseline-trial
  (let [summary (analysis/summarize
                  rows 30000
                  {:bootstrap-samples 40 :bootstrap-seed 99})
        paired  (get-in summary
                        [:paired-runtime-ratios :conditions :raw-t10])]
    (is (= :production-t0
           (get-in summary [:paired-runtime-ratios :baseline-condition])))
    (is (= 3 (get-in paired [:estimate :pairs])))
    (is (= 15.0 (get-in paired [:estimate :p50-ratio])))
    (is (= (/ 2.0 3.0) (get-in paired [:estimate :ge-10-rate])))
    (is (= :hierarchical-paired-query-then-seed-bootstrap
           (get-in paired [:bootstrap :method])))
    (is (vector?
          (get-in paired [:bootstrap :metrics :p99-ratio :ci95])))))

(deftest plan-census-does-not-report-runtime-tail-rates
  (let [plan-rows (mapv #(assoc % :plan-only? true) rows)
        summary   (analysis/summarize
                    plan-rows 30000
                    {:bootstrap-samples 10 :bootstrap-seed 7})]
    (is (zero? (get-in summary
                       [:conditions :raw-t10 :runtime-trials])))
    (is (zero? (get-in summary [:conditions :raw-t10 :catastrophes])))
    (is (nil? (get-in summary
                      [:bootstrap :conditions :raw-t10
                       :catastrophe-rate :estimate])))))

(deftest preregistered-query-cohorts-get-independent-summaries
  (let [cohorts (analysis/summarize-query-cohorts
                  rows 30000
                  {:variable ["1a"]
                   :stable-control ["2a"]}
                  {:bootstrap-samples 10 :bootstrap-seed 7})]
    (is (= 1 (get-in cohorts [:definitions :variable :queries])))
    (is (= 4 (get-in cohorts [:definitions :variable :trials])))
    (is (= 2 (get-in cohorts [:definitions :stable-control :trials])))
    (is (= [] (:unassigned-queries cohorts)))
    (is (= {} (:overlapping-queries cohorts)))
    (is (= 1 (get-in cohorts [:summaries :variable :queries])))
    (is (= 1 (get-in cohorts [:summaries :stable-control :queries])))))

(deftest query-cohorts-reject-unobserved-query-names
  (let [error (try
                (analysis/summarize-query-cohorts
                  rows 30000 {:stale ["missing"]}
                  {:bootstrap-samples 0})
                nil
                (catch clojure.lang.ExceptionInfo exception exception))]
    (is error)
    (is (= {:stale ["missing"]}
           (:unknown-by-cohort (ex-data error))))))
