(ns openrulebench.runner-test
  (:require
   [clojure.test :refer [deftest is]]
   [openrulebench.runner :as runner]))

(deftest orchestrator-argument-test
  (let [defaults (runner/parse-args [])]
    (is (= 1 (:warmup defaults)))
    (is (= 1 (:iterations defaults))))
  (let [opts (runner/parse-args
               ["--systems" "datalevin,sqlite"
                "--warmup" "0"
                "--iterations" "2"
                "tc:tiny" "sg:tiny"])]
    (is (= [:datalevin :sqlite] (:systems opts)))
    (is (= 0 (:warmup opts)))
    (is (= 2 (:iterations opts)))
    (is (= ["tc:tiny" "sg:tiny"] (:benchmarks opts)))))

(deftest orchestrator-keeps-warmup-and-measurement-in-one-child-test
  (let [tasks ["tc:tiny-cyclic-ff" "sg:tiny-cyclic-ff"]
        opts  (runner/parse-args
                ["--systems" "datalevin"
                 "tc:tiny-cyclic-ff" "sg:tiny-cyclic-ff"])
        calls (atom [])]
    (with-redefs-fn
      {#'runner/run-child
       (fn [system child-opts supported]
         (swap! calls conj [(:warmup child-opts)
                            (:iterations child-opts)
                            supported])
         {:host {:child system}
          :results (mapv (fn [spec]
                           {:system (name system)
                            :benchmark spec
                            :status :ok
                            :time-ms 1.0
                            :result-count 1
                            :input-digest spec})
                         supported)})}
      (fn []
        (let [report (#'runner/run-system :datalevin opts)]
          (is (= [[1 1 tasks]] @calls))
          (is (= [{:child :datalevin}] (:child-hosts report)))
          (is (every? #(= :ok (:status %)) (:results report))))))))

(deftest help-does-not-resolve-to-benchmarks-test
  (is (true? (:help? (runner/parse-args ["--help"])))))

(deftest benchmark-groups-cover-published-matrix-test
  (is (= 42 (count runner/all-benchmarks)))
  (is (= 21 (count (:benchmarks (runner/parse-args ["differential"])))))
  (is (= runner/default-benchmarks
         (:benchmarks (runner/parse-args []))))
  (is (thrown? clojure.lang.ExceptionInfo
               (runner/parse-args ["dblp:small"]))))

(deftest system-capabilities-are-task-specific-test
  (is (#'runner/supported? :datalevin "join1:50k-a-bf"))
  (is (#'runner/supported? :sqlite "tc:50k-acyclic-fb"))
  (is (#'runner/supported? :clara "sg:6k-cyclic-ff"))
  (is (not (#'runner/supported? :clara "sg:6k-cyclic-bf")))
  (is (not (#'runner/supported? :odoyle "join1:50k-a-ff"))))

(deftest non-cancellable-backends-isolate-each-task-test
  (let [tasks ["tc:tiny-cyclic-ff" "sg:tiny-cyclic-ff"]]
    (is (= [tasks]
           (#'runner/child-task-groups :datalevin tasks)))
    (is (= [["tc:tiny-cyclic-ff"] ["sg:tiny-cyclic-ff"]]
           (#'runner/child-task-groups :odoyle tasks)))))

(deftest cross-system-input-digest-test
  (is (empty? (#'runner/input-mismatches
                [{:benchmark "tc:tiny" :status :ok :input-digest "same"}
                 {:benchmark "tc:tiny" :status :ok :input-digest "same"}])))
  (is (= [{:benchmark "tc:tiny" :digests #{"a" "b"}}]
         (#'runner/input-mismatches
          [{:benchmark "tc:tiny" :status :ok :input-digest "a"}
           {:benchmark "tc:tiny" :status :ok :input-digest "b"}]))))
