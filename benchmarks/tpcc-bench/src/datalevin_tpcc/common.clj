(ns datalevin-tpcc.common
  "Shared metadata for the TPC-C-derived benchmark.

  This is a derived implementation of the TPC-C specification. It follows the
  published schema and transaction definitions but is not an audited TPC-C
  result."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s])
  (:import
   [java.io File]))

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
