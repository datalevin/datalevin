(ns datalevin-tpcc.sql-invariant-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datalevin-tpcc.check :as check]
   [datalevin-tpcc.datalevin]
   [datalevin-tpcc.postgres]
   [datalevin-tpcc.sqlite]))

(deftest payment-counts-compare-exactly-across-jdbc-numeric-types
  (let [errors (fn [actual expected]
                 (check/payment-errors
                   {} {:customer-payment-cnt {[1 1] actual}} {}
                   {[1 1] {:amount 0.0 :count expected}}))]
    (is (empty? (errors 4264M 4264)))
    (is (seq (errors 4264.1M 4264)) "fractional values must not be truncated")
    (is (seq (errors 9007199254740993M 9007199254740992))
        "integer counts must not be compared as doubles")))

(def ^:private drivers
  '[datalevin-tpcc.datalevin datalevin-tpcc.sqlite datalevin-tpcc.postgres])

(def ^:private order-change {:district-next-o-id 1 :order-count 1})
(def ^:private successful-order
  {:type :new-order :status :ok :changes {[1 1] order-change}})
(def ^:private rolled-back-order {:type :new-order :status :invalid-item})

(defn- run-benchmark
  ([driver actions warmup] (run-benchmark driver actions warmup 1))
  ([driver actions warmup threads]
   (run-benchmark driver actions warmup threads future-call))
  ([driver actions warmup threads start-terminal]
   (let [state (atom (into {} (for [w [1 2] d (range 1 11)]
                                [[w d] {:district-next-o-id 3001 :order-count 3000}])))
         calls (atom 0)
         opened (atom 0)
         closed (atom [])
         open (fn [& _]
                (let [id (swap! opened inc)]
                  (reify java.sql.Connection
                    (close [_] (swap! closed conj id)))))
         transact
         (fn [& _]
           (let [{:keys [type status changes error before]} (nth actions (dec (swap! calls inc)))]
             (when before (before))
             (when error (throw error))
             (doseq [[k deltas] changes [invariant delta] deltas]
               (swap! state update-in [k invariant] + delta))
             (let [res {:status status :amount 1.0}]
               (if (= driver 'datalevin-tpcc.sqlite)
                 res
                 [type {:w 1 :d 1} res 1.0]))))
         read-next (fn [_ w d] (get-in @state [[w d] :district-next-o-id]))
         read-count (fn [_ w d] (get-in @state [[w d] :order-count]))
         dependencies
         {:do-txn transact
          :pick-type (fn [_] (:type (nth actions @calls)))
          :measure! (fn [f] (f)) :start-terminal start-terminal
          :open-conn open :get-connection open
          :close-conn (fn [conn] (.close ^java.sql.Connection conn))
          :enable-wal! (constantly nil)
          :gen-input (constantly {:w 1 :d 1})
          :district-next-o-id read-next :order-count read-count
          :payment-state (constantly {}) :payment-errors (constantly [])}
         outcome (atom nil)
         output
         (with-out-str
           (reset! outcome
                   (try
                     {:result ((ns-resolve driver 'bench)
                               {:warehouses 2 :threads threads :warmup warmup
                                :txns (- (count actions) warmup)} dependencies)}
                     (catch Throwable e {:error e}))))]
     (is (= (count actions) @calls) "all requested transactions ran")
     (is (= (set (range 1 (inc @opened))) (set @closed))
         "all connections close on both success and invariant failure")
     (is (= @opened (count @closed)) "each connection closes exactly once")
     (assoc @outcome :output output))))

(defn- check-failure [driver action expected]
  (let [{:keys [error result output]} (run-benchmark driver [action] 0)]
    (is (instance? clojure.lang.ExceptionInfo error))
    (is (= expected (:errors (ex-data error))))
    (is (nil? result) "corrupt runs do not return benchmark metrics")
    (is (not (str/includes? output "tpmC:")) "corrupt runs do not publish throughput")
    (is (not (str/includes? output "p95=")) "corrupt runs do not publish latency")
    (is (not (str/includes? output "invariant: OK")))))

(deftest active-district-invariant-failures-abort-all-drivers
  (doseq [driver drivers
          invariant [:district-next-o-id :order-count]]
    (testing (str driver " " invariant)
      (check-failure
       driver (assoc-in successful-order [:changes [1 1] invariant] 0)
       [{:invariant invariant :key [1 1]
         :expected (if (= invariant :district-next-o-id) 3002 3001)
         :actual (if (= invariant :district-next-o-id) 3001 3000)}]))))

(deftest untouched-districts-are-checked
  (doseq [driver drivers]
    (testing (str driver)
      (check-failure
       driver (assoc-in successful-order [:changes [2 10]] order-change)
       [{:invariant :district-next-o-id :key [2 10] :expected 3001 :actual 3002}
        {:invariant :order-count :key [2 10] :expected 3000 :actual 3001}]))))

(deftest runs-without-committed-new-orders-still-check-every-district
  (doseq [driver drivers
          [type status] [[:payment :ok] [:new-order :invalid-item]]]
    (testing (str driver " " type " " status)
      (check-failure
       driver {:type type :status status :changes {[2 10] order-change}}
       [{:invariant :district-next-o-id :key [2 10] :expected 3001 :actual 3002}
        {:invariant :order-count :key [2 10] :expected 3000 :actual 3001}]))))

(deftest completed-new-orders-include-required-rollbacks
  (doseq [driver drivers
          [actions warmup completed committed rolled-back]
          [[[successful-order] 0 1 1 0]
           [[rolled-back-order] 0 1 0 1]
           [(conj (vec (repeat 99 successful-order)) rolled-back-order) 0 100 99 1]
           [[successful-order rolled-back-order successful-order rolled-back-order]
            2 2 1 1]
           [[successful-order rolled-back-order] 2 0 0 0]
           [[{:type :payment :status :ok}] 0 0 0 0]
           [[{:type :order-status :status :invalid-item}] 0 0 0 0]
           [[{:type :new-order :status :error} {:type :new-order :status :not-found}]
            0 0 0 0]
           [[] 0 0 0 0]]]
    (testing (str driver " warmup=" warmup " committed=" committed)
      (let [{:keys [result error output]} (run-benchmark driver actions warmup)]
        (is (nil? error))
        (is (= :ok (:invariants result)))
        (is (= completed (:new-orders result)))
        (is (= committed (:committed-new-orders result)))
        (is (= rolled-back (:rolled-back-new-orders result)))
        (is (= (- (count actions) warmup)
               (reduce + 0 (map :count (vals (:stats result))))))
        (is (= (/ (* 60.0 completed) (:elapsed result)) (:tpmc result)))
        (is (str/includes? output (format "New-Orders: %d (%d committed, %d rolled back)"
                                          completed committed rolled-back)))
        (is (str/includes? output "district next_o_id invariant: OK"))
        (is (str/includes? output "order count invariant: OK"))))))

(deftest completed-new-orders-are-combined-across-terminals
  (doseq [driver drivers]
    (let [actions (vec (take 400 (cycle [successful-order rolled-back-order])))
          {:keys [result error]} (run-benchmark driver actions 0 4)]
      (testing (str driver)
        (is (nil? error))
        (is (= 400 (:new-orders result)))
        (is (= 200 (:committed-new-orders result)))
        (is (= 200 (:rolled-back-new-orders result)))
        (is (= (/ (* 60.0 400) (:elapsed result)) (:tpmc result)))
        (is (= :ok (:invariants result)))))))

(deftest unexpected-transaction-failures-do-not-publish-throughput
  (doseq [driver drivers]
    (let [{:keys [result error output]}
          (run-benchmark driver [{:type :new-order
                                  :error (ex-info "Unexpected transaction failure" {})}] 0)]
      (testing (str driver)
        (is (some? error))
        (is (nil? result))
        (is (not (str/includes? output "tpmC:")))))))

(deftest failed-sql-runs-wait-for-every-terminal
  (doseq [driver '[datalevin-tpcc.sqlite datalevin-tpcc.postgres]]
    (testing (str driver)
      (let [failed-started (promise)
            other-started (promise)
            other-active (promise)
            release-other (promise)
            failure (ex-info "Terminal failed" {})
            start-future clojure.core/future-call
            workers (atom [])
            start-terminal
            (fn [f]
              (let [first? (empty? @workers)
                    worker (start-future #(do (when-not first?
                                                (deliver other-started true))
                                              (f)))]
                (swap! workers conj worker)
                worker))
            actions [{:type :payment :error failure
                      :before #(do (deliver failed-started true)
                                   (is (= true (deref other-started 5000 ::timeout))))}
                     {:type :payment :status :ok
                      :before #(do (deliver other-active true) @release-other)}]
            runner (start-future #(run-benchmark driver actions 0 2 start-terminal))]
        (try
          (is (= true (deref other-active 5000 ::timeout)))
          (is (= ::pending (deref runner 250 ::pending))
              "the run must not throw while another terminal is active")
          (deliver release-other true)
          (let [{:keys [error result output] :as outcome}
                (deref runner 5000 ::timeout)]
            (is (not= ::timeout outcome))
            (is (identical? failure (some-> ^Throwable error .getCause))
                "the original terminal failure is propagated after cleanup")
            (is (nil? result))
            (is (not (str/includes? (or output "") "tpmC:")))
            (is (not (str/includes? (or output "") "invariant: OK"))))
          (finally
            (deliver release-other true)
                ;; Also drain workers if an assertion fails against a runner
                ;; that still exits early, before returning from the test.
            (doseq [worker @workers]
              (try (deref worker 5000 ::timeout)
                   (catch Throwable _)))
            (deref runner 5000 ::timeout)))))))
