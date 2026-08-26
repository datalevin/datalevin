(ns openrulebench.core-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [openrulebench.core :as core]
   [openrulebench.data :as data]))

(deftest transitive-closure-reference-test
  (testing "paths and cycles"
    (is (= 3 (core/tc-reference-count [[0 1] [1 2]])))
    (is (= 2 (core/tc-reference-count [[0 1] [1 2]] :bf 0)))
    (is (= 2 (core/tc-reference-count [[0 1] [1 2]] :fb 2)))
    (is (= 4 (core/tc-reference-count [[0 1] [1 0]])))
    (is (= 0 (core/tc-reference-count [])))))

(deftest same-generation-reference-test
  (let [relations {:par [[0 1] [2 3]]
                   :sib [[1 3]]}]
    (is (= 2 (core/sg-reference-count relations)))
    (is (= 1 (core/sg-reference-count relations :bf 0)))
    (is (= 1 (core/sg-reference-count relations :fb 2))))
  (is (= 1 (core/sg-reference-count {:par [] :sib [[2 3]]}))))

(deftest portable-task-parsing-test
  (is (= {:family :tc
          :instance :50k
          :shape :acyclic
          :binding :bf
          :bound-value 1
          :published? true
          :spec "tc:50k-acyclic-bf"}
         (select-keys (core/benchmark-task "tc:50k-acyclic-bf")
                      [:family :instance :shape :binding :bound-value
                       :published? :spec])))
  (is (= {:generator-version 1
          :seed 42
          :nodes 1000
          :edge-facts 50000}
         (:input-profile (core/benchmark-task "tc:50k-acyclic-bf"))))
  (is (= :b2 (:query (core/benchmark-task "join1:250k-b2-fb"))))
  (is (false? (:published?
                (core/benchmark-task "sg:tiny-cyclic-ff"))))
  (is (nil? (core/benchmark-task "dblp:small")))
  (is (thrown? clojure.lang.ExceptionInfo
               (core/require-benchmark-task "lubm:lubm-10"))))

(deftest deterministic-set-generators-test
  (let [graph (data/generate-random-graph 20 50 :seed 7 :acyclic? true)
        join  (data/generate-join1-instance :tiny)
        task  (core/require-benchmark-task "join1:tiny-a-ff")]
    (is (= graph (data/generate-random-graph 20 50
                                             :seed 7 :acyclic? true)))
    (is (= 50 (count graph) (count (set graph))))
    (is (every? (fn [[a b]] (< a b)) graph))
    (doseq [relation [:d1 :d2 :c2 :c3 :c4]]
      (is (= 100 (count (get join relation))))
      (is (= (count (get join relation))
             (count (set (get join relation))))))
    (is (= 64 (count (core/task-data-digest task join))))
    (is (= (core/task-data-digest task join)
           (core/task-data-digest task
                                  (data/generate-join1-instance :tiny))))))

(deftest join1-reference-test
  (let [relations {:d1 [[0 1]]
                   :d2 [[1 2]]
                   :c2 [[2 3]]
                   :c3 [[3 4]]
                   :c4 [[4 5]]}]
    (is (= 1 (core/join1-reference-count relations :a :ff 1)))
    (is (= 1 (core/join1-reference-count relations :a :bf 0)))
    (is (= 1 (core/join1-reference-count relations :a :fb 5)))
    (is (= 1 (core/join1-reference-count relations :b1 :ff 1)))
    (is (= 1 (core/join1-reference-count relations :b2 :ff 1)))))

(deftest repeated-measurement-test
  (let [calls  (atom 0)
        result (core/run-repeated
                 "test" "join1:small"
                 (fn [_]
                   (let [n (swap! calls inc)]
                     {:status :ok :result-count 7 :time-ms (double n)}))
                 {:warmup 1 :iterations 3 :verify? false})]
    (is (= 4 @calls))
    (is (= :ok (:status result)))
    (is (= [2.0 3.0 4.0] (:samples-ms result)))
    (is (= 3.0 (:time-ms result)))
    (is (= :skipped (get-in result [:correctness :status])))))

(deftest job-style-pass-order-test
  (let [tasks ["tc:tiny-cyclic-ff" "sg:tiny-cyclic-ff"]
        calls (atom [])
        results
        (core/run-benchmark-passes
          "test" tasks
          (fn [spec]
            (let [n (count (swap! calls conj spec))]
              {:status :ok
               :result-count 7
               :time-ms (double n)
               :input-digest spec}))
          {:warmup 1 :iterations 1 :verify? false})]
    (is (= ["tc:tiny-cyclic-ff" "sg:tiny-cyclic-ff"
            "tc:tiny-cyclic-ff" "sg:tiny-cyclic-ff"]
           @calls))
    (is (= [[3.0] [4.0]] (mapv :samples-ms results)))
    (is (every? #(= :single-measurement (:reported-statistic %)) results))))

(deftest correctness-and-failure-gates-test
  (let [incorrect (core/run-repeated
                    "test" "tc:tiny"
                    (constantly {:status :ok :result-count 9 :time-ms 1.0})
                    {:warmup 0 :iterations 2 :verify? true})
        calls     (atom 0)
        failed    (core/run-repeated
                    "test" "join1:small"
                    (fn [_]
                      (swap! calls inc)
                      {:status :error})
                    {:warmup 0 :iterations 5 :verify? true})]
    (is (= :incorrect (:status incorrect)))
    (is (= 10000 (:expected-count incorrect)))
    (is (= :error (:status failed)))
    (is (= 1 @calls))))

(deftest wrapped-out-of-memory-test
  (is (core/out-of-memory?
        (ex-info "engine failed: Java heap space" {})))
  (is (core/out-of-memory?
        (ex-info "wrapped" {} (OutOfMemoryError. "heap"))))
  (is (not (core/out-of-memory? (ex-info "ordinary failure" {})))))

(deftest runner-argument-test
  (is (= 1 (:iterations
             (core/parse-run-args [] ["tc:tiny-cyclic-ff"]))))
  (is (= {:warmup 0
          :iterations 3
          :verify? false
          :quiet? false
          :output nil
          :benchmarks ["tc:tiny"]}
         (core/parse-run-args
           ["--warmup" "0" "--iterations" "3" "--no-verify" "tc:tiny"]
           ["tc:small"]))))
