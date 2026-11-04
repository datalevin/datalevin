(ns datalevin-tpcc.warmup-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin-tpcc.datalevin]
   [datalevin-tpcc.postgres]
   [datalevin-tpcc.sqlite])
  (:import [java.util Random]))

(def ^:private drivers
  '[datalevin-tpcc.datalevin datalevin-tpcc.sqlite datalevin-tpcc.postgres])

(defn- warmup-stream [driver seed n & [load-seed]]
  (let [calls (atom [])
        done (ex-info "Warmup complete" {})
        conn (reify java.sql.Connection (close [_]))
        generate (var-get (ns-resolve driver 'gen-input))
        capture (fn [r opts type]
                  (let [input (generate r opts type)]
                    (swap! calls conj {:rng r :type type :input input
                                       :constants (select-keys opts [:c-last :c-cust :c-item])})
                    input))
        dependencies
        {:open-conn (constantly conn) :get-connection (constantly conn)
         :close-conn (constantly nil) :enable-wal! (constantly nil)
         :measure! (fn [f] (f))
         :district-next-o-id (fn [& _] (throw done))
         :gen-input capture
         :do-txn (if (= driver 'datalevin-tpcc.sqlite)
                   (fn [_ _ _] {:status :probe})
                   (fn [_ r opts type]
                     [type (capture r opts type) {:status :probe} 0.0]))}]
    ;; Exercise the driver loop with explicit database dependencies, keeping
    ;; direct linking enabled. Stop at the first baseline read after warmup.
    (with-out-str
      (is (identical? done
                      (try
                        ((ns-resolve driver 'bench)
                         (cond-> {:seed seed :warehouses 1 :warmup n :threads 1 :txns 1}
                           (some? load-seed) (assoc :load-seed load-seed))
                         dependencies)
                        nil
                        (catch clojure.lang.ExceptionInfo e e)))))
    @calls))

(defn- inputs [calls]
  (mapv #(select-keys % [:type :input]) calls))

(deftest surname-constants-match-the-population-in-every-driver
  (doseq [[seed load-seed] [[42 nil] [42 42] [43 42] [42 104729] [12345 123456789]]]
    (let [c-load (.nextInt (Random. (+ (or load-seed 42) 15)) 256)
          streams (mapv #(warmup-stream % seed 100 load-seed) drivers)]
      (when (= seed 42)
        (when (or (nil? load-seed) (= load-seed 42))
          (is (= 186 c-load) "the default population constant remains unchanged")))
      (is (apply = (map inputs streams)) "all drivers generate the same workload")
      (doseq [[driver calls] (map vector drivers streams)]
        (testing (str driver " input seed=" seed " load seed=" load-seed)
          (let [constants (map :constants calls)
                c-run (:c-last (first constants))
                delta (Math/abs (long (- c-load c-run)))]
            (is (= 1 (count (distinct constants))) "constants stay fixed throughout the run")
            (is (<= 0 c-run 255))
            (is (<= 65 delta 119))
            (is (not (#{96 112} delta)))))))))

(deftest warmup-advances-one-random-stream-through-the-full-mix
  (doseq [driver drivers]
    (testing (str driver)
      (let [calls (warmup-stream driver 42 1000)]
        (is (= 1000 (count calls)))
        (is (= #{:new-order :payment :order-status :delivery :stock-level}
               (set (map :type calls))))
        (is (> (count (distinct (inputs calls))) 100))
        (is (= 1 (count (distinct (map :rng calls))))
            "every warmup transaction consumes the same RNG instance")
        (is (= (inputs calls) (inputs (warmup-stream driver 42 1000))))
        (is (not= (inputs calls) (inputs (warmup-stream driver 43 1000))))
        (is (= [] (warmup-stream driver 42 0)))
        (is (= [(first (inputs calls))]
               (inputs (warmup-stream driver 42 1))))))))
