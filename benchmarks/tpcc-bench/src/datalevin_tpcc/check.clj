(ns datalevin-tpcc.check
  "Accounting checks taken before and after the measured transaction interval."
  (:require
   [clojure.set :as set]
   [datalevin.core :as d]
   [datalevin-tpcc.common :as c]))

(defn sql-schema!
  "Reject old comparison databases whose SQL scalar representations differ."
  [conn dialect query]
  (let [actual (if (= dialect :postgres)
                 (into {} (map (fn [[table column type]] [[table column] type]))
                       (query conn "SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_schema=current_schema()"))
                 (into {} (for [table c/table-order
                                [_ column type] (query conn (str "PRAGMA table_info(" table ")"))]
                            [[table column] type])))
        sql-types (if (= dialect :postgres)
                {:long "bigint" :double "double precision" :string "text"}
                {:long "INTEGER" :double "REAL" :string "TEXT"})
        errors (vec (for [[table {:keys [columns types]}] c/table-specs
                          [column type] (map vector columns types)
                          :let [expected (get sql-types type)
                                found (get actual [table column])]
                          :when (not= expected found)]
                      {:table table :column column :expected expected :actual found}))]
    (when (seq errors)
      (throw (ex-info "TPC-C representation mismatch; reload the benchmark database"
                      {:backend dialect :errors errors})))))

(defn payment-state
  "Read Payment accounting totals while benchmark terminals are stopped."
  [conn]
  (let [db (d/db conn)
        customers (d/q '[:find ?w ?d (sum ?amount) (sum ?count)
                         :with ?e
                         :where [?e :customer/key]
                         [?e :customer/district ?district]
                         [?district :district/id ?d]
                         [?district :district/warehouse ?warehouse]
                         [?warehouse :warehouse/id ?w]
                         [?e :customer/ytd-payment ?amount]
                         [?e :customer/payment-cnt ?count]] db)
        history (d/q '[:find ?w ?d (sum ?amount) (count ?e)
                       :where [?e :history/district ?district]
                       [?district :district/id ?d]
                       [?district :district/warehouse ?warehouse]
                       [?warehouse :warehouse/id ?w]
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
                  :where [?e :district/key]
                  [?e :district/warehouse ?warehouse]
                  [?warehouse :warehouse/id ?w]
                  [?e :district/id ?d]
                  [?e :district/ytd ?amount]] db))
     :customer-ytd-payment (amounts customers)
     :customer-payment-cnt (counts customers)
     :history-amount (amounts history)
     :history-count (counts history)}))

(defn sql-payment-state
  "The same accounting projection for SQLite and PostgreSQL. `query` returns rows."
  [conn query]
  (let [customers (query conn "SELECT c_w_id,c_d_id,SUM(c_ytd_payment),SUM(c_payment_cnt) FROM customer GROUP BY c_w_id,c_d_id")
        history (query conn "SELECT h_w_id,h_d_id,SUM(h_amount),COUNT(*) FROM history GROUP BY h_w_id,h_d_id")
        field (fn [rows index]
                (into {} (map (fn [row] [[(nth row 0) (nth row 1)] (nth row index)])) rows))]
    {:warehouse-ytd (into {} (query conn "SELECT w_id,w_ytd FROM warehouse"))
     :district-ytd (field (query conn "SELECT d_w_id,d_id,d_ytd FROM district") 2)
     :customer-ytd-payment (field customers 2) :customer-payment-cnt (field customers 3)
     :history-amount (field history 2) :history-count (field history 3)}))

(defn payment-errors
  "Compare accounting deltas with successful payments keyed by [warehouse district].
  The fourth argument groups payments by customer warehouse/district separately
  from the home location; the three-argument form assumes all payments local.
  Each payment entry contains :amount and :count. Monetary comparisons allow
  less than half a cent of floating point error; counts must match exactly."
  ([before after payments] (payment-errors before after payments payments))
  ([before after payments customer-payments]
   (let [amounts (update-vals payments :amount)
         counts (update-vals payments :count)
         warehouses (reduce-kv (fn [m [w _] amount]
                                 (update m w (fnil + 0.0) amount))
                               {} amounts)
         expected {:warehouse-ytd warehouses
                   :district-ytd amounts
                   :customer-ytd-payment (update-vals customer-payments :amount)
                   :customer-payment-cnt (update-vals customer-payments :count)
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
                         ;; PostgreSQL SUM(bigint) returns an exact decimal.
                         (== actual expected)
                         (< (Math/abs (- (double actual) (double expected)))
                            0.005)))]
        {:invariant invariant :key k :expected expected :actual actual})))))
