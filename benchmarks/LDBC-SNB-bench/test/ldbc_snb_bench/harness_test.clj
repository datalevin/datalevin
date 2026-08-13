(ns ldbc-snb-bench.harness-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ldbc-snb-bench.harness :as harness]))

(def query-def
  {:name "IC1" :description "test query"})

(def entry
  {:query-def query-def
   :query-key :ic1
   :selection-index 0
   :source-index 3
   :params {:person-id 100 :first-name "John"}
   :origin {:kind :bundled-sf1-sample}
   :expected-count 1})

(deftest latency-summary-test
  (is (= {:count 4
          :min 1.0
          :median 2.0
          :p95 4.0
          :p99 4.0
          :max 4.0
          :mean 2.5}
         (harness/latency-summary [4.0 1.0 3.0 2.0]))))

(deftest deterministic-parameter-schedule-test
  (let [all-defs [{:name "IC1"} {:name "IC2"}]
        source {:kind :edn
                :parameters {:ic1 (mapv #(hash-map :person-id %
                                                   :first-name "A")
                                        (range 20))
                             :ic2 (mapv #(hash-map :person-id %
                                                   :max-date (java.util.Date. 0))
                                        (range 20))}
                :origins {:ic1 {:kind :edn} :ic2 {:kind :edn}}}
        opts {:parameter-count 5 :seed 17 :scale-factor "1"}
        first-schedule (harness/build-schedule all-defs all-defs source opts {})
        second-schedule (harness/build-schedule all-defs all-defs source opts {})
        ic2-only (harness/build-schedule all-defs [(second all-defs)]
                                         source opts {})]
    (is (= (mapv #(dissoc % :query-def) first-schedule)
           (mapv #(dissoc % :query-def) second-schedule)))
    (is (= 10 (count first-schedule)))
    (is (= (mapv :source-index (drop 5 first-schedule))
           (mapv :source-index ic2-only)))
    (is (= 5 (count (distinct (map :source-index ic2-only)))))))

(deftest correctness-gated-repetition-test
  (let [calls (atom 0)
        result (harness/benchmark-parameter
                 entry
                 (fn [_]
                   (let [n (swap! calls inc)]
                     {:execution-time (double n)
                      :result-count 1
                      :rows [[100 "John"]]
                      :columns ["id" "name"]}))
                 {:warmup 1 :iterations 3 :verify? true})]
    (is (= 5 @calls))
    (is (= :ok (:status result)))
    (is (= [3.0 4.0 5.0] (:samples-ms result)))
    (is (= 4.0 (:execution-time result)))
    (is (= :passed (get-in result [:correctness :status])))))

(deftest oracle-failure-stops-before-timing-test
  (let [calls (atom 0)
        result (harness/benchmark-parameter
                 (assoc entry :expected-count 2)
                 (fn [_]
                   (swap! calls inc)
                   {:execution-time 1.0
                    :result-count 1
                    :rows [[1]]})
                 {:warmup 4 :iterations 5 :verify? true})]
    (is (= 1 @calls))
    (is (= :incorrect (:status result)))
    (is (= :verification (:phase result)))
    (is (empty? (:samples-ms result)))))

(deftest repeat-digest-failure-test
  (let [calls (atom 0)
        result (harness/benchmark-parameter
                 (dissoc entry :expected-count)
                 (fn [_]
                   (let [n (swap! calls inc)]
                     {:execution-time 1.0
                      :result-count 1
                      :rows [[n]]}))
                 {:warmup 1 :iterations 3 :verify? true})]
    (is (= 2 @calls))
    (is (= :incorrect (:status result)))
    (is (= :warmup (:phase result)))))

(deftest verification-can-be-explicitly-skipped-test
  (let [calls (atom 0)
        result (harness/benchmark-parameter
                 entry
                 (fn [_]
                   (let [n (swap! calls inc)]
                     {:execution-time (double n)
                      :result-count 1
                      :rows [[n]]}))
                 {:warmup 1 :iterations 2 :verify? false})]
    (is (= 3 @calls))
    (is (= :ok (:status result)))
    (is (= [2.0 3.0] (:samples-ms result)))
    (is (= :skipped (get-in result [:correctness :status])))))

(deftest benchmark-argument-test
  (let [opts (harness/parse-bench-args
               ["--warmup" "0" "--iterations" "3"
                "--parameter-count" "2" "--parameters" "params.edn"
                "--no-verify" "is1"])]
    (is (= 0 (:warmup opts)))
    (is (= 3 (:iterations opts)))
    (is (= 2 (:parameter-count opts)))
    (is (= "params.edn" (:parameters opts)))
    (is (false? (:verify? opts)))
    (is (= ["IS1"] (:query-names opts))))
  (testing "invalid and incomplete options fail loudly"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Missing value"
                          (harness/parse-bench-args ["--iterations"])))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unrecognized option"
                          (harness/parse-bench-args ["--wat"])))))
