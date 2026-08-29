(ns datalevin-bench.harness-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check :as check]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [datalevin-bench.harness :as h])
  (:import
   [java.io StringWriter]
   [java.util.concurrent CountDownLatch Executors TimeUnit]))

(defn- run-sync
  [opts]
  (h/run-benchmark!
    (merge {:total-writes 1
            :batch-size 1
            :threads 1
            :report-every 0
            :tx-fn (fn [_ callback] (callback :ok))
            :add-fn #(.add % :item)}
           opts)))

(defn- thrown-info
  [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e e)))

(deftest sync-run-handles-a-partial-final-batch
  (let [seen   (atom [])
        result (h/run-benchmark!
                 {:total-writes 10
                  :batch-size 3
                  :threads 1
                  :report-every 0
                  :tx-fn (fn [txs callback]
                           (swap! seen conj (count txs))
                           (callback :ok))
                  :add-fn #(.add % :item)})]
    (is (= [3 3 3 1] @seen))
    (is (= 10 (:writes result)))
    (is (= 4 (:requests result)))
    (is (pos? (:throughput-writes-sec result)))
    (is (<= 0.0 (get-in result [:completion-latency :p99-ms])))))

(deftest sync-run-supports-multiple-callers
  (let [requests (atom 0)
        result   (h/run-benchmark!
                   {:total-writes 103
                    :batch-size 10
                    :threads 4
                    :report-every 0
                    :tx-fn (fn [_ callback]
                             (swap! requests inc)
                             (callback :ok))
                    :add-fn #(.add % :item)})]
    (is (= 11 @requests))
    (is (= 103 (:writes result)))
    (is (= 11 (:requests result)))))

(deftest batch-accounting-property
  (let [result
        (check/quick-check
          100
          (prop/for-all [total (gen/choose 1 200)
                         batch (gen/choose 1 40)]
            (let [sizes  (atom [])
                  added  (atom 0)
                  result (h/run-benchmark!
                           {:total-writes total
                            :batch-size batch
                            :threads 1
                            :report-every 0
                            :tx-fn (fn [txs callback]
                                     (swap! sizes conj (count txs))
                                     (callback :ok))
                            :add-fn (fn [txs]
                                      (swap! added inc)
                                      (.add txs :item))})]
              (and (= total @added (:writes result) (reduce + @sizes))
                   (= (long (Math/ceil (/ (double total) batch)))
                      (:requests result)
                      (count @sizes))
                   (every? #(<= 1 % batch) @sizes)))))]
    (is (:pass? result) (pr-str result))))

(deftest async-run-awaits-every-callback
  (let [executor (Executors/newSingleThreadExecutor)
        completed (atom 0)]
    (try
      (let [result
            (h/run-benchmark!
              {:total-writes 25
               :batch-size 2
               :threads 1
               :async? true
               :in-flight 4
               :report-every 0
               :completion-timeout-ms 5000
               :tx-fn (fn [_ callback]
                        (.submit executor
                                 ^Runnable
                                 #(do
                                    (Thread/sleep 1)
                                    (swap! completed inc)
                                    (callback :ok))))
               :add-fn #(.add % :item)})]
        (is (= 13 @completed))
        (is (= 25 (:writes result)))
        (is (= 13 (:requests result))))
      (finally
        (.shutdown executor)
        (.awaitTermination executor 5 TimeUnit/SECONDS)))))

(deftest async-run-enforces-the-in-flight-limit
  (let [executor  (Executors/newFixedThreadPool 2)
        started   (CountDownLatch. 2)
        release   (promise)
        active    (atom 0)
        max-active (atom 0)]
    (try
      (let [result
            (h/run-benchmark!
              {:total-writes 6
               :batch-size 1
               :threads 1
               :async? true
               :in-flight 2
               :report-every 0
               :completion-timeout-ms 5000
               :tx-fn (fn [_ callback]
                        (.submit executor
                                 ^Runnable
                                 #(let [n (swap! active inc)]
                                    (swap! max-active max n)
                                    (.countDown started)
                                    (when (.await started 5 TimeUnit/SECONDS)
                                      (deliver release true))
                                    (deref release 5000 false)
                                    (swap! active dec)
                                    (callback :ok))))
               :add-fn #(.add % :item)})]
        (is (= 6 (:writes result)))
        (is (= 2 @max-active))
        (is (zero? @active)))
      (finally
        (deliver release true)
        (.shutdown executor)
        (.awaitTermination executor 5 TimeUnit/SECONDS)))))

(deftest async-errors-fail-the-run
  (testing "an error completion is not mistaken for a successful write"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Benchmark write failed"
          (h/run-benchmark!
            {:total-writes 1
             :batch-size 1
             :threads 1
             :async? true
             :in-flight 1
             :report-every 0
             :completion-timeout-ms 1000
             :tx-fn (fn [_ callback]
                      (callback (ex-info "expected" {:type :test})))
             :add-fn #(.add % :item)})))))

(deftest async-errors-drain-already-submitted-work
  (let [executor (Executors/newSingleThreadExecutor)
        calls    (atom 0)
        drained? (atom false)]
    (try
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"Benchmark write failed"
            (h/run-benchmark!
              {:total-writes 10
               :batch-size 1
               :threads 1
               :async? true
               :in-flight 2
               :report-every 0
               :completion-timeout-ms 5000
               :tx-fn (fn [_ callback]
                        (if (= 1 (swap! calls inc))
                          (.submit executor
                                   ^Runnable
                                   #(do
                                      (Thread/sleep 10)
                                      (reset! drained? true)
                                      (callback :ok)))
                          (callback (ex-info "expected" {:type :test}))))
               :add-fn #(.add % :item)})))
      (is @drained?)
      (is (= 2 @calls))
      (finally
        (.shutdown executor)
        (.awaitTermination executor 5 TimeUnit/SECONDS)))))

(deftest synchronous-submission-errors-stop-new-work
  (let [calls (atom 0)]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Benchmark write failed"
          (h/run-benchmark!
            {:total-writes 10
             :batch-size 1
             :threads 1
             :report-every 0
             :tx-fn (fn [_ _]
                      (swap! calls inc)
                      (throw (ex-info "expected" {:type :test})))
             :add-fn #(.add % :item)})))
    (is (= 1 @calls))))

(deftest batch-construction-errors-stop-before-submission
  (let [calls (atom 0)
        error (thrown-info
                #(run-sync
                   {:total-writes 5
                    :tx-fn (fn [_ callback]
                             (swap! calls inc)
                             (callback :ok))
                    :add-fn (fn [_]
                              (throw (ex-info "bad input" {:type :test})))}))]
    (is (= "Benchmark write failed" (ex-message error)))
    (is (= "bad input" (ex-message (ex-cause error))))
    (is (zero? @calls))))

(deftest success-followed-by-a-throw-invalidates-the-run
  (let [error (thrown-info
                #(run-sync
                   {:tx-fn (fn [_ callback]
                             (callback :ok)
                             (throw (ex-info "after callback" {:type :test})))}))]
    (is (= "Benchmark write failed" (ex-message error)))
    (is (= "after callback" (ex-message (ex-cause error))))))

(deftest duplicate-completions-fail-the-run
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"Benchmark write failed"
        (h/run-benchmark!
          {:total-writes 1
           :batch-size 1
           :threads 1
           :async? true
           :in-flight 1
           :report-every 0
           :completion-timeout-ms 1000
           :tx-fn (fn [_ callback]
                    (callback :ok)
                    (callback :duplicate))
           :add-fn #(.add % :item)}))))

(deftest missing-completions-time-out
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"Timed out waiting for benchmark writes to complete"
        (h/run-benchmark!
          {:total-writes 2
           :batch-size 1
           :threads 1
           :async? true
           :in-flight 1
           :report-every 0
           :completion-timeout-ms 25
           :tx-fn (fn [_ _] nil)
           :add-fn #(.add % :item)}))))

(deftest invalid-run-shapes-fail-before-allocation
  (let [base-opts {:total-writes 1
                   :batch-size 1
                   :threads 1
                   :report-every 0
                   :tx-fn (fn [_ callback] (callback :ok))
                   :add-fn #(.add % :item)}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #":tx-fn must be callable"
                          (h/run-benchmark! (assoc base-opts :tx-fn nil))))
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #":batch-size exceeds the supported integer range"
          (h/run-benchmark!
            (assoc base-opts :batch-size (inc (long Integer/MAX_VALUE))))))))

(deftest all-scalar-options-are-validated
  (doseq [[overrides message]
          [[{:total-writes 0} ":total-writes must be a positive integer"]
           [{:batch-size 0} ":batch-size must be a positive integer"]
           [{:threads 0} ":threads must be a positive integer"]
           [{:in-flight 0} ":in-flight must be a positive integer"]
           [{:completion-timeout-ms 0}
            ":completion-timeout-ms must be a positive integer"]
           [{:report-every -1}
            ":report-every must be a non-negative integer"]
           [{:add-fn nil} ":add-fn must be callable"]
           [{:threads (inc (long Integer/MAX_VALUE))}
            ":threads exceeds the supported integer range"]
           [{:in-flight (inc (long Integer/MAX_VALUE))}
            ":in-flight exceeds the supported integer range"]]]
    (is (= message (ex-message (thrown-info #(run-sync overrides))))))
  (is (= "Too many benchmark requests"
         (ex-message
           (thrown-info
             #(run-sync {:total-writes (inc (long Integer/MAX_VALUE))}))))))

(deftest latency-summary-uses-nearest-rank-percentiles
  (is (= {:mean-ms 2.5
          :p50-ms 2.0
          :p95-ms 4.0
          :p99-ms 4.0}
         (#'h/latency-summary
           (long-array [1000000 2000000 3000000 4000000])))))

(deftest csv-output-is-stable-and-locale-independent
  (let [result {:writes 10
                :requests 4
                :elapsed-seconds 1.25
                :throughput-writes-sec 8.0
                :call-latency {:mean-ms 1.23456}
                :completion-latency {:mean-ms 2.0
                                     :p50-ms 1.0
                                     :p95-ms 3.0
                                     :p99-ms 4.0}}
        row    (h/result->csv-row :measurement result)]
    (is (= (str "measurement,10,4,1.25,8.00,1.2346,2.0000,"
                "1.0000,3.0000,4.0000")
           row))
    (is (= 10 (count (str/split row #","))))))

(deftest progress-goes-only-to-stderr
  (let [out (StringWriter.)
        err (StringWriter.)]
    (binding [*out* out
              *err* err]
      (run-sync {:total-writes 3 :report-every 1}))
    (is (= "" (str out)))
    (is (= 3 (count (re-seq #"Completed" (str err)))))))

(deftest print-result-emits-one-header-and-one-row
  (let [out    (StringWriter.)
        result {:writes 1
                :requests 1
                :elapsed-seconds 1.0
                :throughput-writes-sec 1.0
                :call-latency {:mean-ms 1.0}
                :completion-latency {:mean-ms 1.0
                                     :p50-ms 1.0
                                     :p95-ms 1.0
                                     :p99-ms 1.0}}]
    (binding [*out* out]
      (h/print-result! :measurement result))
    (let [lines (str/split-lines (str out))]
      (is (= 2 (count lines)))
      (is (= h/csv-header (first lines)))
      (is (str/starts-with? (second lines) "measurement,1,1,")))))
