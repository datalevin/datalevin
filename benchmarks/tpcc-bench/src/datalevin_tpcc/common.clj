(ns datalevin-tpcc.common
  "Shared metadata for the TPC-C-derived benchmark.

  This is a derived implementation of the TPC-C specification. It follows the
  published schema and transaction definitions but is not an audited TPC-C
  result."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s])
  (:import
   [java.io File]
   [java.util Locale]))

(def base-dir
  (or (System/getenv "TPCC_DIR") "."))

(defn data-dir ^File [] (io/file base-dir "data"))

(defn query-file ^File [dialect n]
  (io/file base-dir "queries" (name dialect) (str n ".sql")))

;; ---------------------------------------------------------------------------
;; Load parameters (fixed by the specification)

(def item-count 100000)
(def districts-per-warehouse 10)
(def customers-per-district 3000)
(def initial-orders-per-district 3000)
(def new-orders-per-district 900)

;; ---------------------------------------------------------------------------
;; Shared transaction calculations

(defn new-order-total
  "Apply the customer discount and home warehouse/district taxes to the sum
  of OL_AMOUNT values (TPC-C 2.4.2.2). Rates are fractions, not percentages."
  [subtotal c-discount w-tax d-tax]
  (* (double subtotal)
     (- 1.0 (double c-discount))
     (+ 1.0 (double w-tax) (double d-tax))))

(defn middle-customer
  "Select from customers already sorted by first name, or return nil if empty.
  TPC-C 2.5.2.2 chooses the lower middle customer when the count is even."
  [customers]
  (when (seq customers)
    (nth customers (quot (dec (count customers)) 2))))

(defn order-status-result
  "Materialize the TPC-C 2.6.2.2 fields in the same shape for every backend.
  Rows are customer [id balance first middle last], order [id entry carrier],
  and lines [number item supply quantity amount delivery], in line-number order.
  Keep the line count for callers that only need the transaction summary."
  [[c-id balance first-name middle last-name] [o-id entry carrier] lines]
  (let [order-lines
        (mapv (fn [[number item supply quantity amount delivery]]
                {:number (long number) :i-id (long item)
                 :supply-w-id (long supply) :quantity (long quantity)
                 :amount (double amount) :delivery-d delivery})
              lines)]
    {:type :order-status :status :ok
     :customer {:id (long c-id) :balance (double balance)
                :first first-name :middle middle :last last-name}
     :order {:id (long o-id) :entry-d entry
             :carrier-id (some-> carrier long)}
     :lines (count order-lines) :order-lines order-lines}))

(defn bad-credit-data
  "Prepend the six Payment history fields required by TPC-C 2.5.2.2 to C_DATA,
  retaining at most 500 characters. IDs are customer, customer district,
  customer warehouse, payment district, and payment warehouse, in that order."
  [c-id c-d-id c-w-id d-id w-id amount old-data]
  (let [prefix (String/format Locale/ROOT "%d %d %d %d %d %.2f | "
                              (to-array [c-id c-d-id c-w-id d-id w-id (double amount)]))
        data   (str prefix old-data)]
    (subs data 0 (min 500 (count data)))))

(defn payment-history-data
  "Build H_DATA from the payment warehouse and district names (TPC-C 2.5.2.2)."
  [warehouse-name district-name]
  (str warehouse-name "    " district-name))

(defn stock-after-lines
  "Apply TPC-C 2.4.2 stock updates in order to [quantity ytd order-cnt remote-cnt].
  `quantities` contains the order-line quantities for one supplying warehouse
  and item. Replenishment and both counters apply separately to every line."
  [stock quantities remote?]
  (reduce
   (fn [[quantity ytd order-cnt remote-cnt] ordered]
     (let [quantity  (long quantity)
           ordered   (long ordered)
           remaining (- quantity ordered)]
       [(if (>= quantity (+ ordered 10)) remaining (+ remaining 91))
        (+ (long ytd) ordered)
        (inc (long order-cnt))
        (+ (long remote-cnt) (if remote? 1 0))]))
   (mapv long stock) quantities))

;; ---------------------------------------------------------------------------
;; Measured transaction results

(defn new-order-metrics
  "Summarize measured [type latency-ms status] records over `elapsed` seconds.
  TPC-C 5.1.2 counts both committed New-Orders and required invalid-item
  rollbacks as completed transactions. Other statuses do not contribute to
  tpmC. Callers must exclude warmup records."
  [samples elapsed]
  (let [statuses    (frequencies (for [[type _ status] samples
                                      :when (= type :new-order)]
                                  status))
        committed   (get statuses :ok 0)
        rolled-back (get statuses :invalid-item 0)
        completed   (+ committed rolled-back)]
    {:new-orders completed
     :committed-new-orders committed
     :rolled-back-new-orders rolled-back
     :tpmc (/ (* 60.0 completed) elapsed)}))

;; ---------------------------------------------------------------------------
;; Table metadata

(def table-order
  ["item" "warehouse" "stock" "district" "customer" "history"
   "orders" "new_order" "order_line"])

(def table-specs
  (array-map
   "item"
   {:columns ["i_id" "i_im_id" "i_name" "i_price" "i_data"]
    :types   [:long :long :string :double :string]}

   "warehouse"
   {:columns ["w_id" "w_ytd" "w_tax" "w_name" "w_street_1" "w_street_2"
              "w_city" "w_state" "w_zip"]
    :types   [:long :double :double :string :string :string :string :string
              :string]}

   "stock"
   {:columns ["s_i_id" "s_w_id" "s_quantity"
              "s_dist_01" "s_dist_02" "s_dist_03" "s_dist_04" "s_dist_05"
              "s_dist_06" "s_dist_07" "s_dist_08" "s_dist_09" "s_dist_10"
              "s_ytd" "s_order_cnt" "s_remote_cnt" "s_data"]
    :types   [:long :long :long
              :string :string :string :string :string
              :string :string :string :string :string
              :long :long :long :string]}

   "district"
   {:columns ["d_id" "d_w_id" "d_ytd" "d_tax" "d_next_o_id" "d_name"
              "d_street_1" "d_street_2" "d_city" "d_state" "d_zip"]
    :types   [:long :long :double :double :long :string :string :string
              :string :string :string]}

   "customer"
   {:columns ["c_id" "c_d_id" "c_w_id" "c_first" "c_middle" "c_last"
              "c_street_1" "c_street_2" "c_city" "c_state" "c_zip"
              "c_phone" "c_since" "c_credit" "c_credit_lim" "c_discount"
              "c_balance" "c_ytd_payment" "c_payment_cnt" "c_delivery_cnt"
              "c_data"]
    :types   [:long :long :long :string :string :string :string :string
              :string :string :string :string :string :string :double
              :double :double :double :long :long :string]}

   "history"
   {:columns ["h_c_id" "h_c_d_id" "h_c_w_id" "h_d_id" "h_w_id" "h_date"
              "h_amount" "h_data"]
    :types   [:long :long :long :long :long :string :double :string]}

   "orders"
   {:columns ["o_id" "o_d_id" "o_w_id" "o_c_id" "o_entry_d" "o_carrier_id"
              "o_ol_cnt" "o_all_local"]
    :types   [:long :long :long :long :string :long :long :long]}

   "new_order"
   {:columns ["no_o_id" "no_d_id" "no_w_id"]
    :types   [:long :long :long]}

   "order_line"
   {:columns ["ol_o_id" "ol_d_id" "ol_w_id" "ol_number" "ol_i_id"
              "ol_supply_w_id" "ol_delivery_d" "ol_quantity" "ol_amount"
              "ol_dist_info"]
    :types   [:long :long :long :long :long :long :string :long :double
              :string]}))

(defn- col->attr
  [table col]
  (keyword (s/replace table "_" "-")
           (s/replace (subs col (inc (.indexOf ^String col "_"))) "_" "-")))

(defn attrs [table]
  (mapv #(col->attr table %) (:columns (table-specs table))))

(defn stock-dist-column
  "The S_DIST_xx column for the ordering district, restricted to the schema."
  [district]
  (when-not (and (integer? district) (<= 1 district districts-per-warehouse))
    (throw (ex-info "Invalid stock district" {:district district})))
  (nth (:columns (table-specs "stock")) (+ 2 district)))

(defn stock-dist-attr
  "The Datalevin attribute corresponding to the ordering district's S_DIST_xx."
  [district]
  (col->attr "stock" (stock-dist-column district)))

(defn type->db-type [t]
  (case t
    :long   :db.type/long
    :double :db.type/double
    :string :db.type/string))

(defn parse-value
  [t v]
  (cond
    (nil? v) nil
    (= t :long)   (long v)
    (= t :double) (double v)
    :else         (str v)))

(defn close-enough?
  ([a b] (close-enough? a b 1.0e-9))
  ([a b tol]
   (if (and (number? a) (number? b))
     (let [a (double a) b (double b)
           scale (max 1.0 (Math/abs a) (Math/abs b))]
       (<= (Math/abs (- a b)) (* tol scale)))
     (= a b))))
