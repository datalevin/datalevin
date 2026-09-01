(ns idoc-bench.core-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [idoc-bench.core :as bench]))

(defn- call-private
  [symbol & args]
  (apply (ns-resolve 'idoc-bench.core symbol) args))

(deftest deterministic-schedule-test
  (let [opts      (assoc bench/default-opts :records 50 :ops 30 :warmup 0)
        schedule  (call-private 'generate-schedule opts 30 99)
        same      (call-private 'generate-schedule opts 30 99)
        different (call-private 'generate-schedule opts 30 100)]
    (is (= 30 (count schedule)))
    (is (= schedule same))
    (is (not= schedule different))
    (is (= (call-private 'schedule-digest schedule)
           (call-private 'schedule-digest same)))
    (is (every? #(contains? % :op) schedule))))

(deftest independent-query-oracle-test
  (let [doc {:profile {:lang "en"}
             :stats {:score 0.5}
             :facts {"city" "SF" "team" "blue"}
             :events [{:entity {:name "acme"} :tags ["urgent"]}]}
        matches? #(call-private 'reference-match? doc %)]
    (testing "strict range bounds"
      (is (matches? {:type :range :lo 0.49 :hi 0.51}))
      (is (not (matches? {:type :range :lo 0.5 :hi 0.6})))
      (is (not (matches? {:type :range :lo 0.4 :hi 0.5}))))
    (is (matches? {:type :nested :value "en"}))
    (is (matches? {:type :wildcard-one :value "blue"}))
    (is (matches? {:type :wildcard-depth :value "acme"}))
    (is (matches? {:type :array :value "urgent"}))))

(deftest latency-summary-test
  (let [summary (call-private
                  'latency-summary
                  (mapv #(hash-map :latency-ns %) [1000000 2000000 3000000 4000000]))]
    (is (= 4 (:count summary)))
    (is (= 2.0 (:median summary)))
    (is (= 4.0 (:p95 summary)))
    (is (= 2.5 (:mean summary)))))

(deftest option-validation-test
  (is (map? (call-private 'validate-options bench/default-opts)))
  (is (= "--threads must not exceed --ops"
         (call-private 'validate-options
                       (assoc bench/default-opts :ops 1 :threads 2))))
  (is (= "--hotset must be greater than 0 and at most 1"
         (call-private 'validate-options
                       (assoc bench/default-opts :hotset 0.0))))
  (is (= "unsupported durability profile: :unknown"
         (call-private 'validate-options
                       (assoc bench/default-opts
                              :durability-profile :unknown))))
  (is (= "unsupported run role: :unknown"
         (call-private 'validate-options
                       (assoc bench/default-opts :run-role :unknown))))
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"--repetitions is not supported"
        (call-private 'parse-args ["--repetitions" "3"]))))

(deftest benchmark-query-projects-only-ids-test
  (is (= '[:find ?e
           :in $ ?q
           :where [(idoc-match $ :mem/doc ?q nil) [[?e _ _]]]]
         bench/q-idoc)))

(deftest single-pass-summary-test
  (let [latency {:operations {:read {:count 10 :mean 1.0}}
                 :idoc-shapes {}}
        result  (call-private
                  'summarize-pass
                  [{:system :datalevin
                    :throughput-ops-per-second 20.0
                    :elapsed-ms 500.0
                    :latency-ms latency}])]
    (is (= :single-complete-pass (:aggregation result)))
    (is (= 20.0
           (get-in result
                   [:systems :datalevin :throughput-ops-per-second])))
    (is (= latency
           (get-in result [:systems :datalevin :latency-ms])))))

(deftest two-pass-pair-validation-test
  (let [base {:format-version 3
              :configuration {:system :datalevin :workload :A}
              :schedule {:measurement-sha256 "same"}
              :results [{:system :datalevin
                         :correctness {:status :passed}}]
              :host {:jvm-instance-id "warmup-jvm"
                     :benchmark-host-control "controlled"}}
        warmup (assoc base :protocol {:run-role :warmup})
        measurement (-> base
                        (assoc :protocol {:run-role :measurement})
                        (assoc-in [:host :jvm-instance-id]
                                  "measurement-jvm"))]
    (is (true? (bench/validate-pass-pair warmup measurement)))
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Invalid IDoc benchmark warmup/measurement pair"
          (bench/validate-pass-pair
            warmup
            (assoc-in measurement [:schedule :measurement-sha256]
                      "different"))))))
