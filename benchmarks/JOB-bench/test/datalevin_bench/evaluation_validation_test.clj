(ns datalevin-bench.evaluation-validation-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin-bench.evaluation-validation :as validation]))

(def timing
  [{:run 0 :query "1a" :sample-seed 11 :mode :no-floor
    :status :ok :plan-hash 10}
   {:run 0 :query "1a" :sample-seed 11 :mode :shrink
    :status :ok :plan-hash 10}])

(defn estimate
  [condition policy fingerprint]
  {"Phase" "timing"
   "Run" "0"
   "Seed" "7"
   "Sample Seed" "11"
   "Position" (if (= condition "no-floor") "0" "1")
   "Condition" condition
   "Base Mode" (if (= condition "no-floor") "full" "shrink-only")
   "Estimator Policy" policy
   "Baseline" "false"
   "Query Name" "1a"
   "Observation" "0"
   "Ratio Key" "[:_ref :movie/id]"
   "Link Type" "_ref"
   "Attribute" ":movie/id"
   "Index" ":ave"
   "Sample Size" "1000"
   "Sample Population" "10000"
   "Sample Fingerprint" fingerprint
   "Mean" "0.5"
   "Final Ratio" "1.0"})

(def health
  [{:phase :timing :pass 0 :moment :before}
   {:phase :timing :pass 0 :moment :after
    :failed? false
    :contamination-reasons []
    :vm-delta {:swapouts 0}}])

(def manifests
  [{:status :running}
   {:status :complete
    :config {:queries ["1a"]
             :conditions [{:name :no-floor} {:name :shrink}]
             :runs 1}}])

(deftest accepts-deterministic-equivalent-control
  (let [report
        (validation/validate-artifacts
          {:timing timing
           :estimates [(estimate "no-floor" "production" "sample-a")
                       (estimate "shrink" "shrink" "sample-a")]
           :health health
           :manifests manifests
           :equivalent-condition-pairs [[:no-floor :shrink]]})]
    (is (:accepted? report))
    (is (zero? (get-in report [:paired-seeds :mismatch-count])))
    (is (zero? (get-in report
                       [:sample-fingerprints :mismatch-count])))
    (is (= 1 (get-in report
                     [:equivalent-controls 0 :plan :match-count])))
    (is (= 1 (get-in report
                     [:equivalent-controls 0 :estimator :match-count])))))

(deftest rejects-sample-and-health-contamination
  (let [bad-health (assoc-in health [1 :vm-delta :swapouts] 3)
        report
        (validation/validate-artifacts
          {:timing timing
           :estimates [(estimate "no-floor" "production" "sample-a")
                       (estimate "shrink" "shrink" "sample-b")]
           :health bad-health
           :manifests manifests})]
    (is (false? (:accepted? report)))
    (is (= 1 (get-in report
                     [:sample-fingerprints :mismatch-count])))
    (is (= 1 (get-in report [:health :swapout-pass-count])))))

(deftest distinct-sampling-configurations-are-not-conflated
  (let [small (assoc (estimate "no-floor" "production" "sample-a")
                     "Sample Budget" "250"
                     "Input Size" "2000")
        large (assoc (estimate "shrink" "shrink" "sample-b")
                     "Sample Budget" "1000"
                     "Input Size" "8000")
        report
        (validation/validate-artifacts
          {:timing timing
           :estimates [small large]
           :health health
           :manifests manifests})]
    (is (:accepted? report))
    (is (zero? (get-in report
                       [:sample-fingerprints :shared-sample-sources])))
    (is (zero? (get-in report
                       [:sample-fingerprints :mismatch-count])))))

(deftest rejects-incomplete-pairs-and-estimator-controls
  (testing "missing timing condition"
    (is (false?
          (:accepted?
            (validation/validate-artifacts
              {:timing (pop timing)
               :estimates []
               :health health
               :manifests manifests})))))
  (testing "algebraic control estimator mismatch"
    (is (false?
          (:accepted?
            (validation/validate-artifacts
              {:timing timing
               :estimates
               [(estimate "no-floor" "production" "sample-a")
                (assoc (estimate "shrink" "shrink" "sample-a")
                       "Final Ratio" "2.0")]
               :health health
               :manifests manifests
               :equivalent-condition-pairs [[:no-floor :shrink]]}))))))

(deftest rejects-empty-estimator-or-health-artifacts
  (is (false?
        (:accepted?
          (validation/validate-artifacts
            {:timing timing
             :estimates []
             :health health
             :manifests manifests}))))
  (is (false?
        (:accepted?
          (validation/validate-artifacts
            {:timing timing
             :estimates [(estimate "no-floor" "production" "sample-a")
                         (estimate "shrink" "shrink" "sample-a")]
             :health []
             :manifests manifests})))))

(deftest rejects-incomplete-diagnostic-replay
  (let [report
        (validation/validate-artifacts
          {:timing timing
           :diagnostics [(assoc (first timing)
                                :plan-only? true
                                :plan-hash 10)]
           :estimates [(estimate "no-floor" "production" "sample-a")
                       (estimate "shrink" "shrink" "sample-a")]
           :health health
           :manifests manifests})]
    (is (false? (:accepted? report)))
    (is (false? (get-in report [:diagnostics :valid?])))))
