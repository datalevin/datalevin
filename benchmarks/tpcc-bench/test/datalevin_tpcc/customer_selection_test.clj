(ns datalevin-tpcc.customer-selection-test
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
    (let [dir (u/tmp-dir (str "datalevin-tpcc-customer-" (random-uuid)))]
      (try
        (binding [*test-dir* dir] (f))
        (finally
          (when (.exists (u/file dir))
            (u/delete-files dir)))))))

(def customer-cases
  ;; IDs in first-name order, expected customer ID, expected order-line count.
  [[[] nil nil]
   [[91] 91 1]
   [[82 81] 82 1]
   [[73 72 71] 72 2]
   [[64 63 62 61] 63 2]
   [[55 54 53 52 51] 53 3]
   [[46 45 44 43 42 41] 44 3]])

(defn- last-name [ids] (str "MATCHES" (count ids)))

(defn- seed-rows []
  (let [customer (first (g/customer-rows 42 1))
        customers (for [[ids] customer-cases
                        ;; Insert in reverse first-name order as well.
                        [rank cid] (reverse (map-indexed vector ids))]
                    {:cid cid :rank (inc rank) :last (last-name ids)})
        date "2026-01-01T00:00:00"]
    {"warehouse" (take 1 (g/warehouse-rows 42 1))
     "district" (take 1 (g/district-rows 42 1))
     "customer" (for [{:keys [cid rank last]} customers]
                  (assoc customer 0 cid 3 (format "FIRST%02d" rank) 5 last 13 "GC"))
     "orders" (for [{:keys [cid rank]} customers]
                [cid 1 1 cid date 1 rank 1])
     "order_line" (for [{:keys [cid rank]} customers n (range 1 (inc rank))]
                    [cid 1 1 n 1 1 date 5 0.0 "distinfo"])}))

(defn- check-customer-selection!
  [{:keys [payment! order-status! balances history-customers]}]
  (doseq [[ids selected lines] customer-cases]
    (testing (str (count ids) " surname matches")
      (let [input {:w 1 :d 1 :c 9999 :last-name (last-name ids) :by-name? true}]
        (is (= (if selected
                 {:type :order-status :status :ok :lines lines}
                 {:type :order-status :status :no-customer})
               (select-keys (order-status! input) [:type :status :lines])))
        (is (= (if selected {:status :ok :c selected} {:status :no-customer})
               (select-keys (payment! (assoc input :amount 5.0)) [:status :c]))))))
  (let [selected (set (keep second customer-cases))]
    (is (= (into {} (for [[ids] customer-cases cid ids]
                     [cid (if (selected cid) -15.0 -10.0)]))
           (balances))
        "only the selected customers receive the payments")
    (is (= (zipmap selected (repeat 1)) (frequencies (history-customers)))
        "each payment records its selected customer in history")))

(deftest datalevin-customer-selection
  (let [conn (d/get-conn *test-dir* datalevin/schema)]
    (try
      (d/transact! conn
                   (vec (for [[table rows] (seed-rows) row rows]
                          (zipmap (c/attrs table) row))))
      (check-customer-selection!
       {:payment! #(t/payment! conn %)
        :order-status! #(t/order-status! conn %)
        :balances #(into {} (d/q '[:find ?id ?balance
                                   :where [?e :customer/id ?id]
                                          [?e :customer/balance ?balance]]
                                 (d/db conn)))
        :history-customers #(map second (d/q '[:find ?h ?cid
                                              :where [?h :history/c-id ?cid]]
                                            (d/db conn)))})
      (finally (d/close conn)))))

(defn- check-sql-customers!
  [^Connection conn dialect payment! order-status!]
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
  (check-customer-selection!
   {:payment! #(payment! conn %)
    :order-status! #(order-status! conn %)
    :balances #(into {} (map (fn [[cid balance]] [cid (double balance)]))
                     (#'sqlite/qall conn "SELECT c_id, c_balance FROM customer"))
    :history-customers #(map first (#'sqlite/qall conn "SELECT h_c_id FROM history"))}))

(deftest sqlite-customer-selection
  (with-open [conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
    (check-sql-customers! conn "sqlite" sqlite/payment! sqlite/order-status!)))

(deftest postgres-customer-selection
  (if-let [url (System/getenv "TPCC_TEST_PG_URL")]
    (with-open [conn (DriverManager/getConnection
                     url (or (System/getenv "TPCC_TEST_PG_USER") (System/getenv "USER"))
                     (or (System/getenv "TPCC_TEST_PG_PASS") ""))]
      (let [schema (str "tpcc_customer_" (str/replace (str (random-uuid)) "-" ""))]
        (#'sqlite/exec! conn (str "CREATE SCHEMA " schema))
        (try
          (#'sqlite/exec! conn (str "SET search_path TO " schema))
          (check-sql-customers! conn "postgres" postgres/payment! postgres/order-status!)
          (finally
            (when-not (.getAutoCommit conn) (.rollback conn))
            (.setAutoCommit conn true)
            (#'sqlite/exec! conn (str "DROP SCHEMA " schema " CASCADE"))))))
    (println "Skipping PostgreSQL customer selection test; set TPCC_TEST_PG_URL to enable it.")))
