(ns datalevin-tpcc.order-status-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin-tpcc.common :as c]
   [datalevin-tpcc.datalevin :as datalevin]
   [datalevin-tpcc.generate :as g]
   [datalevin-tpcc.postgres :as postgres]
   [datalevin-tpcc.sqlite :as sqlite]
   [datalevin-tpcc.txns :as t])
  (:import
   [java.sql Connection DriverManager]))

(def ^:dynamic *test-dir* nil)

(use-fixtures :each
  (fn [f]
    (let [dir (u/tmp-dir (str "datalevin-tpcc-order-status-" (random-uuid)))]
      (try
        (binding [*test-dir* dir] (f))
        (finally
          (when (.exists (u/file dir))
            (u/delete-files dir)))))))

(defn- seed-rows []
  (let [customer (first (g/customer-rows 42 1))]
    {"customer"
     (for [[w district cid first-name balance]
           [[1 1 3 "CY" 3.0] [1 1 7 "BEA" -123.45] [1 1 9 "ADA" 98.76]
            [2 1 7 "OTHER-W" 777.0] [1 2 7 "OTHER-D" 888.0]]]
       (assoc customer 0 cid 1 district 2 w 3 first-name 4 "OE"
              5 "SHARED" 16 balance))
     ;; Order 30 is newest by ID, even though order 20 has a later date.
     "orders" [[20 1 1 7 "2026-02-01T01:02:03" 4 1 1]
               [30 1 1 7 "2026-01-01T01:02:03" nil 3 0]
               [40 1 1 9 "2026-03-01T01:02:03" 6 2 0]
               [900 1 2 7 "2026-04-01T01:02:03" 8 1 1]
               [900 2 1 7 "2026-04-01T01:02:03" 9 1 1]]
     "order_line"
     [[20 1 1 1 999 1 "2026-02-02T00:00:00" 1 999.0 "distinfo"]
      ;; Insert out of line-number order; lines 1 and 2 have identical details.
      [30 1 1 3 303 1 nil 9 123.45 "distinfo"]
      [30 1 1 2 101 2 nil 4 12.5 "distinfo"]
      [30 1 1 1 101 2 nil 4 12.5 "distinfo"]
      [40 1 1 2 505 2 "2026-03-03T00:00:00" 2 20.25 "distinfo"]
      [40 1 1 1 404 1 "2026-03-02T00:00:00" 7 70.5 "distinfo"]
      ;; Matching order IDs in other warehouses/districts must be excluded.
      [30 1 2 1 888 2 nil 8 888.0 "distinfo"]
      [30 2 1 1 777 1 nil 7 777.0 "distinfo"]]}))

(def ^:private undelivered-status
  {:type :order-status :status :ok :lines 3
   :customer {:id 7 :balance -123.45 :first "BEA" :middle "OE" :last "SHARED"}
   :order {:id 30 :entry-d "2026-01-01T01:02:03" :carrier-id nil}
   :order-lines [{:number 1 :i-id 101 :supply-w-id 2 :quantity 4
                  :amount 12.5 :delivery-d nil}
                 {:number 2 :i-id 101 :supply-w-id 2 :quantity 4
                  :amount 12.5 :delivery-d nil}
                 {:number 3 :i-id 303 :supply-w-id 1 :quantity 9
                  :amount 123.45 :delivery-d nil}]})

(def ^:private delivered-status
  {:type :order-status :status :ok :lines 2
   :customer {:id 9 :balance 98.76 :first "ADA" :middle "OE" :last "SHARED"}
   :order {:id 40 :entry-d "2026-03-01T01:02:03" :carrier-id 6}
   :order-lines [{:number 1 :i-id 404 :supply-w-id 1 :quantity 7
                  :amount 70.5 :delivery-d "2026-03-02T00:00:00"}
                 {:number 2 :i-id 505 :supply-w-id 2 :quantity 2
                  :amount 20.25 :delivery-d "2026-03-03T00:00:00"}]})

(defn- check-order-status! [order-status!]
  (testing "ID and surname lookup retrieve the same complete latest order"
    (doseq [input [{:w 1 :d 1 :c 7}
                   {:w 1 :d 1 :c 9 :by-name? true :last-name "SHARED"}]]
      (let [result (order-status! input)]
        (is (= undelivered-status result))
        (is (vector? (:order-lines result)) "line details are fully materialized"))))
  (testing "delivered order retains its carrier and each line's delivery date"
    (is (= delivered-status (order-status! {:w 1 :d 1 :c 9}))))
  (testing "missing customers and orders retain their result statuses"
    (is (= {:type :order-status :status :no-order}
           (order-status! {:w 1 :d 1 :c 3})))
    (doseq [input [{:w 1 :d 1 :c 999}
                   {:w 1 :d 1 :by-name? true :last-name "MISSING"}]]
      (is (= {:type :order-status :status :no-customer}
             (order-status! input))))))

(deftest datalevin-order-status
  (let [conn (d/get-conn *test-dir* datalevin/schema)]
    (try
      (d/transact! conn
                   (vec (for [[table rows] (seed-rows) row rows]
                          (into {} (remove (comp nil? val))
                                (zipmap (c/attrs table) row)))))
      (check-order-status! #(t/order-status! conn %))
      (finally (d/close conn)))))

(defn- check-sql-order-status! [^Connection conn dialect order-status!]
  (with-open [stmt (.createStatement conn)]
    (doseq [sql (-> (slurp (io/file (c/data-dir) (str "schema-" dialect ".sql")))
                   (str/replace #"(?m)--[^\n]*" "")
                   (str/split #";"))
            :when (not (str/blank? sql))]
      (.execute stmt sql)))
  (doseq [[table rows] (seed-rows)
          :let [sql (#'sqlite/insert-sql table (:columns (c/table-specs table)))]
          row rows]
    (apply #'sqlite/exec! conn sql row))
  (check-order-status! #(order-status! conn %)))

(deftest sqlite-order-status
  (with-open [conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
    (check-sql-order-status! conn "sqlite" sqlite/order-status!)))

(deftest postgres-order-status
  (if-let [url (System/getenv "TPCC_TEST_PG_URL")]
    (with-open [conn (DriverManager/getConnection
                     url (or (System/getenv "TPCC_TEST_PG_USER") (System/getenv "USER"))
                     (or (System/getenv "TPCC_TEST_PG_PASS") ""))]
      (let [schema (str "tpcc_order_status_" (str/replace (str (random-uuid)) "-" ""))]
        (#'sqlite/exec! conn (str "CREATE SCHEMA " schema))
        (try
          (#'sqlite/exec! conn (str "SET search_path TO " schema))
          (check-sql-order-status! conn "postgres" postgres/order-status!)
          (finally
            (when-not (.getAutoCommit conn) (.rollback conn))
            (.setAutoCommit conn true)
            (#'sqlite/exec! conn (str "DROP SCHEMA " schema " CASCADE"))))))
    (println "Skipping PostgreSQL Order-Status test; set TPCC_TEST_PG_URL to enable it.")))
