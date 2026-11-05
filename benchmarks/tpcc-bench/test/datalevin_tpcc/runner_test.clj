(ns datalevin-tpcc.runner-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin-tpcc.runner :as runner]))

(defn- dependencies [open close transact]
  {:open-conn open :close-conn close :do-txn transact
   :pick-type (constantly :stock-level) :measure! (fn [f] (f))
   :district-next-o-id (constantly 3001) :order-count (constantly 3000)
   :payment-state (constantly {}) :payment-errors (constantly [])})

(deftest every-terminal-keeps-its-own-warmed-connection-and-thread
  (let [next-id (atom 0) calls (atom []) closed (atom [])
        result (atom nil)]
    (with-out-str
      (reset! result
              (runner/bench {:threads 3 :warmup 4 :txns 9}
                (dependencies
                  #(swap! next-id inc) #(swap! closed conj %)
                  (fn [conn _ options type]
                    (swap! calls conj [conn (.getId (Thread/currentThread)) (:terminal-d options)])
                    [type {} {:status :ok} 1.0])))))
    (is (= 4 @next-id) "one control connection and three independent terminal connections")
    (is (= #{1 2 3 4} (set @closed)))
    (is (= 4 (count @closed)))
    (is (= {2 7, 3 7, 4 7} (frequencies (map first @calls)))
        "each terminal uses the same connection for its four warmup and three measured calls")
    (is (= 3 (count (set (map second @calls)))))
    (is (every? #(= 1 (count (set (map second %)))) (vals (group-by first @calls))))
    (is (= 9 (get-in @result [:stats :stock-level :count])))))

(deftest throughput-clock-excludes-setup-warmup-baselines-verification-and-close
  (let [clock (atom 0) calls (atom 0) result (atom nil)
        tick #(swap! clock + 1000000000)
        deps (dependencies
               #(do (tick) (Object.)) (fn [_] (tick))
               (fn [_ _ _ type]
                 (swap! calls inc) (tick)
                 [type {} {:status :ok} 1.0]))]
    (with-out-str
      (reset! result
              (runner/bench {:threads 1 :warmup 3 :txns 2}
                (assoc deps :nano-time #(deref clock)
                       :payment-state (fn [_] (tick) {})))))
    (is (= 5 @calls))
    (is (= 2.0 (:elapsed @result)))
    (is (= 11.0 (/ @clock 1e9)))))

(deftest warmup-failure-releases-other-terminals-and-closes-every-connection
  (let [next-id (atom 0) closed (atom []) failure (ex-info "warmup failed" {})]
    (with-out-str
      (is (thrown? Exception
                   (runner/bench {:threads 3 :warmup 1 :txns 3}
                     (dependencies
                       #(swap! next-id inc) #(swap! closed conj %)
                       (fn [conn _ _ type]
                         (when (= conn 2) (throw failure))
                         [type {} {:status :ok} 1.0]))))))
    (is (= 4 @next-id))
    (is (= #{1 2 3 4} (set @closed)))
    (is (= 4 (count @closed)))))
