(ns datalevin-tpcc.runner
  "One connection and worker per terminal, retained through warmup and measurement."
  (:require [datalevin-bench.host :as host]
            [datalevin-tpcc.common :as c]
            [datalevin-tpcc.generate :as g])
  (:import [java.util Random]))

(defn- add-payment [totals location amount]
  (-> totals
      (update-in [location :amount] (fnil + 0.0) amount)
      (update-in [location :count] (fnil inc 0))))

(defn- run-terminal [conn terminal n record? options do-txn pick-type]
  (let [rng (Random. (+ (:seed options) terminal 1))
        input-options (g/terminal-options
                        (assoc (g/run-constants (:seed options) (:load-seed options))
                               :warehouses (:warehouses options)) terminal)]
    (loop [left n result {:samples [] :committed {} :payments {} :customer-payments {}}]
      (if (zero? left)
        result
        (let [[type input response ms] (do-txn conn rng input-options (pick-type rng))
              ok? (= :ok (:status response))
              location [(:w input) (:d input)]
              result (if record?
                       (cond-> (update result :samples conj [type ms (:status response)])
                         (and ok? (= type :new-order))
                         (update-in [:committed location] (fnil inc 0))
                         (and ok? (= type :payment))
                         (update :payments add-payment location (:amount response))
                         (and ok? (= type :payment))
                         (update :customer-payments add-payment
                                 [(:c-w response) (:c-d response)] (:amount response)))
                       result)]
          (recur (dec left) result))))))

(defn- await-all [signals]
  (let [results (mapv deref signals)]
    (if-let [error (some :error results)] (throw error) results)))

(defn- percentile [xs p]
  (nth xs (min (dec (count xs)) (int (Math/floor (* p (count xs)))))))

(defn- summarize [results elapsed]
  (let [samples (into [] (mapcat :samples) results)
        totals (fn [key]
                 (apply merge-with #(merge-with + %1 %2) (map key results)))
        stats (into {}
                    (for [[type samples] (group-by first samples)
                          :let [xs (vec (sort (map second samples)))]]
                      [type {:count (count xs) :mean (/ (reduce + 0.0 xs) (count xs))
                             :p50 (percentile xs 0.5) :p95 (percentile xs 0.95)
                             :p99 (percentile xs 0.99)}]))]
    (assoc (c/new-order-metrics samples elapsed)
           :elapsed elapsed :stats stats
           :committed (apply merge-with + (map :committed results))
           :payments (totals :payments) :customer-payments (totals :customer-payments))))

(defn bench
  "Warm every terminal on its own connection and retain it for measurement.
  `:warmup` is per terminal; `:txns` is the total measured transaction count.
  Open, warmup, baseline reads, verification, and close are outside timing.
  Dependencies are explicit so lifecycle and failure behavior can be tested."
  [options {:keys [open-conn close-conn do-txn pick-type district-next-o-id order-count
                   payment-state payment-errors measure! start-terminal nano-time]
            :or {measure! (fn [f] (host/with-paused-media (f)))
                 nano-time #(System/nanoTime)
                 start-terminal future-call}}]
  (let [{:keys [warehouses threads warmup txns] :as options}
        (merge {:warehouses 1 :threads 1 :warmup 1000 :txns 100000
                :seed 42 :load-seed 42} options)
        _ (when-not (and (pos-int? warehouses) (pos-int? threads)
                         (nat-int? warmup) (nat-int? txns))
            (throw (ex-info "Invalid TPC-C run dimensions" options)))
        ;; Validate constants before opening connections or starting workers.
        _ (g/run-constants (:seed options) (:load-seed options))
        control (open-conn)
        start (promise)
        release (promise)
        ready (mapv (fn [_] (promise)) (range threads))
        finished (mapv (fn [_] (promise)) (range threads))
        workers (atom [])]
    (try
      (println (format "TPC-C-derived: %d warehouse(s), %d terminal(s), %d txns; %d warmup/terminal"
                       warehouses threads txns warmup))
      (measure!
        (fn []
          (doseq [terminal (range threads)]
            (swap! workers conj
                   (start-terminal
                     (fn []
                       (try
                         (let [conn (open-conn)]
                           (try
                             (run-terminal conn terminal warmup false options do-txn pick-type)
                             (deliver (ready terminal) {:ready true})
                             (when @start
                               (let [n (+ (quot txns threads)
                                          (if (= terminal (dec threads)) (mod txns threads) 0))
                                     result (run-terminal conn terminal n true options do-txn pick-type)]
                                 (deliver (finished terminal) {:result result})
                                 ;; Keep close and its synchronization outside the clock.
                                 @release))
                             (finally (close-conn conn))))
                         (catch Throwable error
                           (deliver (ready terminal) {:error error})
                           (deliver (finished terminal) {:error error})
                           (throw error)))))))
          (await-all ready)
          (let [districts (for [w (range 1 (inc warehouses)) d (range 1 11)] [w d])
                read-orders (fn [read] (into {} (map (fn [[w d :as key]]
                                                     [key (read control w d)])) districts))
                next-before (read-orders district-next-o-id)
                orders-before (read-orders order-count)
                payment-before (payment-state control)
                t0 (nano-time)
                _ (deliver start true)
                results (await-all finished)
                elapsed (/ (- (nano-time) t0) 1e9)
                result (summarize (mapv :result results) elapsed)
                order-errors
                (vec (for [[w d :as key] districts
                           [kind before actual]
                           [[:district-next-o-id next-before (district-next-o-id control w d)]
                            [:order-count orders-before (order-count control w d)]]
                           :let [expected (+ (get before key) (get (:committed result) key 0))]
                           :when (not= expected actual)]
                       {:invariant kind :key key :expected expected :actual actual}))
                errors (into order-errors
                             (payment-errors payment-before (payment-state control)
                                             (:payments result) (:customer-payments result)))]
            (when (seq errors)
              (throw (ex-info "TPC-C accounting invariant failure" {:errors errors})))
            (println "district next_o_id invariant: OK")
            (println "order count invariant: OK")
            (println "Payment accounting invariants: OK")
            (println (format "Elapsed: %.2fs  New-Orders: %d (%d committed, %d rolled back)  tpmC: %.1f"
                             elapsed (:new-orders result) (:committed-new-orders result)
                             (:rolled-back-new-orders result) (:tpmc result)))
            (doseq [[type {:keys [count mean p50 p95 p99]}] (sort-by key (:stats result))]
              (println (format "  %-13s n=%-6d mean=%7.3fms p50=%7.3f p95=%7.3f p99=%7.3f"
                               (name type) count mean p50 p95 p99)))
            (assoc (dissoc result :committed) :invariants :ok
                   :protocol (select-keys options [:warehouses :threads :warmup :txns :seed :load-seed])))))
      (finally
        (deliver start false)
        (deliver release true)
        ;; Always drain all workers, even after setup, warmup, or body failure.
        (let [errors (into [] (keep (fn [worker] (try @worker nil (catch Throwable e e)))) @workers)]
          (try (close-conn control)
               (finally
                 (when-let [error (first errors)] (throw error)))))))))
