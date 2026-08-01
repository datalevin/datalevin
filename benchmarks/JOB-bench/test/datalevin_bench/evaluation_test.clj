(ns datalevin-bench.evaluation-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin-bench.evaluation :as evaluation]))

(def queries
  [{:name "1a" :symbol 'q-1a}
   {:name "26c" :symbol 'q-26c}])

(def modes [:full :sampling-only :fallback-only])

(def defaults
  {:sample-size 1000
   :prior-size 100
   :variance-alpha 0.4
   :tail-weight 0.0
   :conservative-lower-bound? true})

(def conditions
  (#'evaluation/selected-conditions nil modes defaults))

(defn- schedule [pass-sample-seed]
  (#'evaluation/schedule queries conditions 0 20260727 pass-sample-seed))

(deftest deterministic-paired-sample-schedule
  (let [first-schedule  (schedule 1001)
        replay          (schedule 1001)
        second-schedule (schedule 1002)]
    (testing "a pass schedule and all of its sample seeds replay exactly"
      (is (= first-schedule replay)))
    (testing "all conditions for a query use the same sample"
      (doseq [[_ entries] (group-by #(get-in % [:query :name])
                                    first-schedule)]
        (is (= 1 (count (distinct (map :sample-seed entries)))))))
    (testing "queries get independent samples"
      (is (= (count queries)
             (count (distinct (map :sample-seed first-schedule))))))
    (testing "a new pass changes every query's sample"
      (let [seeds-by-query
            (fn [entries]
              (into {} (map (juxt #(get-in % [:query :name])
                                  :sample-seed)
                            entries)))]
        (is (every? (fn [[query seed]]
                      (not= seed
                            (get (seeds-by-query second-schedule) query)))
                    (seeds-by-query first-schedule)))))))

(deftest anchored-default-modes
  (is (= evaluation/default-modes
         (#'evaluation/selected-modes nil)))
  (is (= [:full :raw-sampling :shrink-skew]
         (#'evaluation/selected-modes
           ["full" :raw-sampling "shrink-skew"]))))

(deftest tail-pseudo-count-is-part-of-condition
  (let [conditions
        (#'evaluation/selected-conditions
         [{:name :production-t0
           :mode :full
           :tail-weight 0.0
           :baseline? true}
          {:name :production-t10
           :mode :full
           :tail-weight 10.0}]
         nil defaults)]
    (is (= [:production-t0 :production-t10]
           (mapv :name conditions)))
    (is (= [0.0 10.0] (mapv :tail-weight conditions)))
    (is (= [true false] (mapv :baseline? conditions)))
    (is (= 2 (count (distinct (map :name conditions)))))))

(deftest estimator-observation-export
  (let [entry {:run 2
               :seed 10
               :sample-seed 11
               :position 3
               :mode :raw-t10
               :condition
               (first
                 (#'evaluation/selected-conditions
                  [{:name :raw-t10
                    :mode :raw-sampling
                    :tail-weight 10.0
                    :baseline? true}]
                  nil defaults))
               :query {:name "26c"}
               :estimator-observations
               [{:ratio-key [:_ref :movie/id]
                 :link-type :_ref
                 :attr :movie/id
                 :index :ave
                 :sample-size 100
                 :input-size 200
                 :estimated-output 600.0
                 :n 100
                 :sum 300.0
                 :sumsq 1100.0
                 :max-val 20.0
                 :mean 3.0
                 :variance 2.0
                 :cv2 0.25
                 :base-ratio 2.0
                 :prior-size 100.0
                 :var-alpha 0.4
                 :k-eff 110.0
                 :blended 2.5
                 :no-tail-center 3.0
                 :center 3.0
                 :tail-adjustment 0.0
                 :lower-bound 0.0
                 :final-ratio 4.7}]}
        row   (first (#'evaluation/estimator-rows :timing entry))]
    (is (= ["timing" 2 10 11 3 "raw-t10" "raw-sampling" "raw"
            1000 100 0.4 10.0 true true "26c" 0]
           (subvec (vec row) 0 16)))
    (is (= 4.7 (last row)))))

(deftest zero-tail-weight-is-a-recorded-condition-not-a-rejection
  (let [conditions
        (#'evaluation/selected-conditions
         [{:name :production-t0 :mode :full :tail-weight 0.0}
          {:name :skew-t0 :mode :skew-only :tail-weight 0.0}
          {:name :skew-t10 :mode :skew-only :tail-weight 10.0}]
         nil defaults)
        notes (#'evaluation/estimator-config-notes conditions)]
    (is (= 3 (count conditions)))
    (is (= 2 (count notes)))
    (is (some #(re-find #"production-t0" %) notes))
    (is (some #(re-find #"skew-t0" %) notes))))

(deftest invalid-condition-parameters-are-rejected
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"tail pseudo-count"
        (#'evaluation/selected-conditions
         [{:name :bad-tail :mode :full :tail-weight -1.0}]
         nil defaults)))
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"sample budget"
        (#'evaluation/selected-conditions
         [{:name :bad-sample :mode :full :sample-size 0}]
         nil defaults))))

(deftest aggregate-result-fingerprint-uses-returned-result
  (let [value #{["Amy Adams" "Arrival"]}
        small-input {:actual-result-size 20 :result value}
        large-input {:actual-result-size 22 :result value}]
    (is (= (#'evaluation/result-fingerprint small-input)
           (#'evaluation/result-fingerprint large-input)))
    (is (= [1 (hash value)]
           (#'evaluation/result-fingerprint small-input)))))

(deftest structural-plan-hash-ignores-estimator-telemetry
  (let [first-plan  [{:steps ["a" "b"] :size 10.0 :cost 2.0}
                     {:steps ["a" "b" "c"] :size 5.0 :cost 3.0}]
        same-plan   [{:steps ["a" "b"] :size 100.0 :cost 20.0}
                     {:steps ["a" "b" "c"] :size 50.0 :cost 30.0}]
        same-gensym [{:steps ["Initialize [?bound123] by x"]
                      :size 10.0 :cost 2.0}]
        other-gensym [{:steps ["Initialize [?bound987] by x"]
                       :size 100.0 :cost 20.0}]
        same-blank [{:steps ["Initialize [?blank36025] by x"]
                     :size 10.0 :cost 2.0}]
        other-blank [{:steps ["Initialize [?blank36030] by x"]
                      :size 100.0 :cost 20.0}]
        other-plan  [{:steps ["b" "a"] :size 10.0 :cost 2.0}
                     {:steps ["b" "a" "c"] :size 5.0 :cost 3.0}]]
    (is (= (#'evaluation/structural-plan-hash first-plan)
           (#'evaluation/structural-plan-hash same-plan)))
    (is (not= (#'evaluation/structural-plan-hash first-plan)
              (#'evaluation/structural-plan-hash other-plan)))
    (is (= (#'evaluation/structural-plan-hash same-gensym)
           (#'evaluation/structural-plan-hash other-gensym)))
    (is (= (#'evaluation/structural-plan-hash same-blank)
           (#'evaluation/structural-plan-hash other-blank)))))

(deftest timeout-row-does-not-invent-a-plan
  (let [row (zipmap
              evaluation/timing-header
              (#'evaluation/timing-row
               {:run 0
                :seed 1
                :sample-seed 2
                :position 0
                :mode :raw-sampling
                :query {:name "26c"}
                :plan-only? true
                :status "timeout"
                :error "deadline"}))]
    (is (nil? (row "Plan Hash")))
    (is (nil? (row "Plan Decisions (Base64 EDN)")))
    (is (= "timeout" (row "Status")))))

(deftest plain-query-timing-row-separates-measurement-from-diagnostics
  (let [row (zipmap
              evaluation/timing-header
              (#'evaluation/timing-row
               {:run 0
                :seed 1
                :sample-seed 2
                :position 0
                :mode :full
                :query {:name "1a"}
                :plan-only? false
                :result {:timing-method :plain-query
                         :query-time "12.500"
                         :actual-result-size 3}
                :fingerprint [3 99]
                :status "ok"}))]
    (is (= "plain-query" (row "Timing Method")))
    (is (= "12.500" (row "Query Time (ms)")))
    (is (nil? (row "Execution Time (ms)")))
    (is (nil? (row "Plan Hash")))))

(deftest warm-up-accepts-timeout-as-a-tail-outcome
  (is (#'evaluation/acceptable-warm-up-status? "ok"))
  (is (#'evaluation/acceptable-warm-up-status? "timeout"))
  (is (false? (#'evaluation/acceptable-warm-up-status? "error"))))

(deftest restarted-worker-replays-the-next-exact-trial-before-recording
  (let [calls         (atom [])
        needs-rewarm? (atom true)
        entry         {:mode :full :query {:name "1a"}}
        prewarm-entry {:mode :full :query {:name "6d"}}
        request       (fn [request-entry _]
                        (swap! calls conj request-entry)
                        {:status "ok"
                         :fingerprint [1 2]
                         :estimator-observations []})
        result (#'evaluation/request-with-rewarm
                 #(request % nil) entry prewarm-entry needs-rewarm? true 2)]
    (is (= [prewarm-entry entry entry entry] @calls))
    (is (:worker-rewarmed? result))
    (is (false? @needs-rewarm?))))

(deftest rewarm-timeout-is-the-recorded-tail-outcome
  (let [calls         (atom 0)
        needs-rewarm? (atom true)
        result
        (#'evaluation/request-with-rewarm
          (fn [_]
            (if (= 1 (swap! calls inc))
              {:status "ok" :estimator-observations []}
              {:status "timeout" :estimator-observations []}))
          {:mode :no-sampling :query {:name "10c"}}
          {:mode :no-sampling :query {:name "6d"}}
          needs-rewarm? true 2)]
    (is (= 2 @calls))
    (is (= "timeout" (:status result)))
    (is (:worker-rewarmed? result))))
