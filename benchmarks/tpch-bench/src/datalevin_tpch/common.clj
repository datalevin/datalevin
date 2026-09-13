(ns datalevin-tpch.common
  "Shared paths, TPC-H table metadata, and value parsing.

  All three system loaders read the same pipe-delimited .tbl files produced by
  the TPC-H dbgen tool so every comparison runs on identical tables."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin.core :as d])
  (:import
   [java.io File]))

;; ---------------------------------------------------------------------------
;; Paths

(def base-dir
  "Project directory. Override with the TPCH_DIR environment variable."
  (or (System/getenv "TPCH_DIR") "."))

(defn data-dir ^File [] (io/file base-dir "data"))

(defn tbl-dir ^File [] (io/file (data-dir) "tbl"))

(defn tbl-file ^File [table] (io/file (tbl-dir) (str table ".tbl")))

(defn query-file
  "SQL file for `dialect` (:postgres or :sqlite) and query number."
  ^File [dialect n]
  (io/file base-dir "queries" (name dialect) (str n ".sql")))

(defn require-data!
  "Fail early with an actionable message when dbgen output is absent."
  []
  (when-not (.exists (tbl-dir))
    (throw
      (ex-info
        (str "TPC-H data not found at " (.getPath (tbl-dir))
             ". Run scripts/generate-data.sh [scale] first.")
        {:tbl-dir (.getPath (tbl-dir))}))))

;; ---------------------------------------------------------------------------
;; Table metadata
;;
;; Column order matches the dbgen .tbl files. Types drive both Datalevin schema
;; declarations and value parsing:
;;   :long   -> :db.type/long    (keys, sizes, counts)
;;   :double -> :db.type/double  (money, quantity, discounts)
;;   :string -> :db.type/string  (names, codes, ISO dates)

(def table-order
  ["region" "nation" "part" "supplier" "partsupp" "customer" "orders"
   "lineitem"])

(def table-specs
  (array-map
   "region"
   {:columns ["r_regionkey" "r_name" "r_comment"]
    :types   [:long :string :string]}

   "nation"
   {:columns ["n_nationkey" "n_name" "n_regionkey" "n_comment"]
    :types   [:long :string :long :string]}

   "part"
   {:columns ["p_partkey" "p_name" "p_mfgr" "p_brand" "p_type" "p_size"
              "p_container" "p_retailprice" "p_comment"]
    :types   [:long :string :string :string :string :long :string :double
              :string]}

   "supplier"
   {:columns ["s_suppkey" "s_name" "s_address" "s_nationkey" "s_phone"
              "s_acctbal" "s_comment"]
    :types   [:long :string :string :long :string :double :string]}

   "partsupp"
   {:columns ["ps_partkey" "ps_suppkey" "ps_availqty" "ps_supplycost"
              "ps_comment"]
    :types   [:long :long :long :double :string]}

   "customer"
   {:columns ["c_custkey" "c_name" "c_address" "c_nationkey" "c_phone"
              "c_acctbal" "c_mktsegment" "c_comment"]
    :types   [:long :string :string :long :string :double :string :string]}

   "orders"
   {:columns ["o_orderkey" "o_custkey" "o_orderstatus" "o_totalprice"
              "o_orderdate" "o_orderpriority" "o_clerk" "o_shippriority"
              "o_comment"]
    :types   [:long :long :string :double :string :string :string :long
              :string]}

   "lineitem"
   {:columns ["l_orderkey" "l_partkey" "l_suppkey" "l_linenumber" "l_quantity"
              "l_extendedprice" "l_discount" "l_tax" "l_returnflag"
              "l_linestatus" "l_shipdate" "l_commitdate" "l_receiptdate"
              "l_shipinstruct" "l_shipmode" "l_comment"]
    :types   [:long :long :long :long :double :double :double :double :string
              :string :string :string :string :string :string :string]}))

(defn- col->attr
  "Map a TPC-H column name to a namespaced Datalevin attribute keyword,
  e.g. \"l_extendedprice\" in lineitem -> :lineitem/extendedprice."
  [table col]
  (keyword table (s/replace (subs col (inc (.indexOf ^String col "_"))) "_" "-")))

(defn attrs
  "Datalevin attribute keywords for a table, in column order."
  [table]
  (let [{:keys [columns]} (table-specs table)]
    (mapv #(col->attr table %) columns)))

(defn type->db-type
  [t]
  (case t
    :long   :db.type/long
    :double :db.type/double
    :string :db.type/string))

;; ---------------------------------------------------------------------------
;; Parsing

(defn trim-row
  "dbgen appends a trailing '|' to every line; read-csv surfaces it as a final
  empty field."
  [row]
  (if (and (seq row) (= "" (peek row)))
    (pop row)
    row))

(defn parse-value
  [t ^String v]
  (if (nil? v)
    nil
    (case t
      :long   (when (seq v) (Long/parseLong v))
      :double (when (seq v) (Double/parseDouble v))
      v)))

(defn tbl-rows
  "Run `f` with a lazy sequence of trimmed field vectors for `table`. The file
  reader stays open for the dynamic extent of `f`, so `f` must realize what it
  needs before returning."
  [table f]
  (with-open [rdr (io/reader (tbl-file table))]
    (f (map trim-row (d/read-csv rdr :separator \| :quote \")))))

;; ---------------------------------------------------------------------------
;; Result comparison helpers

(defn close-enough?
  "Numeric comparison for TPC-H aggregates, which differ across systems by
  floating-point rounding. Non-numeric values must be equal."
  ([a b] (close-enough? a b 1.0e-6))
  ([a b tol]
   (cond
     (and (number? a) (number? b))
     (let [a (double a) b (double b)
           scale (max 1.0 (Math/abs a) (Math/abs b))]
       (<= (Math/abs (- a b)) (* tol scale)))

     (and (string? a) (string? b)) (= a b)
     :else (= a b))))
