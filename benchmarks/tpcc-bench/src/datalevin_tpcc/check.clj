(ns datalevin-tpcc.check
  "Accounting checks taken before and after the measured transaction interval."
  (:require
   [clojure.set :as set]
   [datalevin.core :as d]))

(defn payment-state
  "Read Payment accounting totals while benchmark terminals are stopped."
  [conn]
  (let [db (d/db conn)
        customers (d/q '[:find ?w ?d (sum ?amount) (sum ?count)
                          :with ?e
                          :where [?e :customer/w-id ?w]
                                 [?e :customer/d-id ?d]
                                 [?e :customer/ytd-payment ?amount]
                                 [?e :customer/payment-cnt ?count]] db)
        history (d/q '[:find ?w ?d (sum ?amount) (count ?e)
                        :where [?e :history/w-id ?w]
                               [?e :history/d-id ?d]
                               [?e :history/amount ?amount]] db)
        amounts (fn [rows]
                  (into {} (map (fn [[w d amount _]] [[w d] amount])) rows))
        counts (fn [rows]
                 (into {} (map (fn [[w d _ n]] [[w d] n])) rows))]
    {:warehouse-ytd
     (into {} (d/q '[:find ?w ?amount
                     :where [?e :warehouse/id ?w]
                            [?e :warehouse/ytd ?amount]] db))
     :district-ytd
     (into {} (map (fn [[w d amount]] [[w d] amount]))
           (d/q '[:find ?w ?d ?amount
                   :where [?e :district/w-id ?w]
                          [?e :district/id ?d]
                          [?e :district/ytd ?amount]] db))
     :customer-ytd-payment (amounts customers)
     :customer-payment-cnt (counts customers)
     :history-amount (amounts history)
     :history-count (counts history)}))

(defn payment-errors
  "Compare accounting deltas with successful payments keyed by [warehouse district].
  Each payment entry contains :amount and :count. Monetary comparisons allow
  less than half a cent of floating point error; counts must match exactly."
  [before after payments]
  (let [amounts (update-vals payments :amount)
        counts (update-vals payments :count)
        warehouses (reduce-kv (fn [m [w _] amount]
                                (update m w (fnil + 0.0) amount))
                              {} amounts)
        expected {:warehouse-ytd warehouses
                  :district-ytd amounts
                  :customer-ytd-payment amounts
                  :customer-payment-cnt counts
                  :history-amount amounts
                  :history-count counts}]
    (vec
     (for [[invariant totals] expected
           :let [old (get before invariant)
                 new (get after invariant)
                 count? (#{:customer-payment-cnt :history-count} invariant)]
           k (set/union (set (keys old)) (set (keys new)) (set (keys totals)))
           :let [actual (- (get new k 0) (get old k 0))
                 expected (get totals k 0)]
           :when (not (if count?
                        (= actual expected)
                        (< (Math/abs (- (double actual) (double expected)))
                           0.005)))]
       {:invariant invariant :key k :expected expected :actual actual}))))
