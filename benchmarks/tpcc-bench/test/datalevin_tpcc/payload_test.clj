(ns datalevin-tpcc.payload-test
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
   [java.sql Connection DriverManager]
   [java.util Locale Locale$Category]))

(def ^:dynamic *test-dir* nil)

(use-fixtures :each
  (fn [f]
    (let [dir (u/tmp-dir (str "datalevin-tpcc-payload-" (random-uuid)))]
      (try
        (binding [*test-dir* dir] (f))
        (finally
          (when (.exists (u/file dir))
            (u/delete-files dir)))))))

(def ^:private old-data (apply str (take 500 (cycle "abcdefghijklmnopqrstuvwxyz"))))

(defn- dist-info [w item district]
  (format "W%02dI%02dD%02d%015d" w item district 0))

(defn- seed-rows []
  (let [customer (first (g/customer-rows 42 1))
        stock (first (g/stock-rows 42 1))]
    {"warehouse" (for [row (g/warehouse-rows 42 2)]
                   (assoc row 3 (str "WAREHOUSE" (first row))))
     "district" (for [row (g/district-rows 42 2) :when (= 2 (second row))]
                  (assoc row 5 (str "DISTRICT" (first row))))
     "customer" (for [district (range 1 11)
                      [cid first-name credit data]
                      [[17 "ALICE" "BC" old-data] [29 "ZOE" "GC" "good-credit-data"]]]
                  (assoc customer 0 cid 1 district 2 2 3 first-name 5 "SHARED"
                         13 credit 20 data))
     "item" (take 2 (g/item-rows 42))
     "stock" (for [w [1 2] item [1 2]]
               (reduce (fn [row district]
                         (assoc row (+ 2 district) (dist-info w item district)))
                       (assoc stock 0 item 1 w)
                       (range 1 11)))}))

(deftest payment-payload-formatting
  (let [locale (Locale/getDefault Locale$Category/FORMAT)]
    (try
      (Locale/setDefault Locale$Category/FORMAT Locale/FRANCE)
      (is (= "11 22 33 44 55 12.34 | previous"
             (c/bad-credit-data 11 22 33 44 55 12.34 "previous"))
          "all five IDs retain their order and amounts always use a decimal point")
      (let [prefix "11 22 33 44 55 1.00 | "
            data (c/bad-credit-data 11 22 33 44 55 1.0 old-data)]
        (is (= 500 (count data)))
        (is (= (str prefix (subs old-data 0 (- 500 (count prefix)))) data)))
      (finally (Locale/setDefault Locale$Category/FORMAT locale))))
  (is (= "WAREHOUSE2    DISTRICT10"
         (c/payment-history-data "WAREHOUSE2" "DISTRICT10"))))

(defn- check-payloads!
  [{:keys [new-order! order-lines payment! customer-data history]}]
  (testing "S_DIST_xx comes from the supplying stock row and ordering district"
    (doseq [district (range 1 11)]
      (let [lines (mapv (fn [[w item]] {:supply-w w :i-id item :qty 5})
                        [[2 1] [1 1] [2 2] [1 2] [2 1]])
            result (new-order! {:w 2 :d district :c 17 :ol lines})]
        (is (= :ok (:status result)))
        (is (= (mapv (fn [n {:keys [supply-w i-id]}]
                       [(inc n) i-id supply-w (dist-info supply-w i-id district)])
                     (range) lines)
               (order-lines district (:o-id result)))))))
  (testing "Payment stores selected IDs, amount, prior C_DATA, and current names"
    (let [expected-history (atom [])]
      (doseq [district [1 7 10]
              [input selected prefix]
              [[{:by-name? true :c 29 :amount 12.34} 17
                (str "17 " district " 2 " district " 2 12.34 | ")]
               [{:by-name? false :c 17 :amount 5000.0} 17
                (str "17 " district " 2 " district " 2 5000.00 | ")]
               [{:by-name? false :c 29 :amount 1.01} 29 nil]]]
        (let [before (customer-data district selected)
              result (payment! (merge {:w 2 :d district :last-name "SHARED"} input))
              after (customer-data district selected)
              expected (if prefix (subs (str prefix before) 0 500) before)]
          (is (= :ok (:status result)))
          (is (= selected (:c result)) "surname selection uses the real customer ID")
          (is (= expected after) "only bad-credit data is prepended and truncated")
          (is (= "good-credit-data" (customer-data district 29)))
          (swap! expected-history conj
                 [selected district 2 district 2 (:amount input)
                  (str "WAREHOUSE2    DISTRICT" district)])))
      (is (= (set @expected-history) (set (history))))
      (is (= 9 (count (history)))))))

(deftest datalevin-transaction-payloads
  (let [conn (d/get-conn *test-dir* datalevin/schema)]
    (try
      (d/transact! conn
                   (vec (for [[table rows] (seed-rows) row rows]
                          (zipmap (c/attrs table) row))))
      (check-payloads!
       {:new-order! #(t/new-order! conn %)
        :payment! #(t/payment! conn %)
        :order-lines
        (fn [district order]
          (vec (sort (d/q '[:find ?n ?item ?supply ?info :in $ ?d ?o
                            :where [?e :order-line/w-id 2]
                                   [?e :order-line/d-id ?d] [?e :order-line/o-id ?o]
                                   [?e :order-line/number ?n] [?e :order-line/i-id ?item]
                                   [?e :order-line/supply-w-id ?supply]
                                   [?e :order-line/dist-info ?info]]
                          (d/db conn) district order))))
        :customer-data
        (fn [district customer]
          (d/q '[:find ?data . :in $ ?d ?c
                 :where [?e :customer/w-id 2] [?e :customer/d-id ?d]
                        [?e :customer/id ?c] [?e :customer/data ?data]]
               (d/db conn) district customer))
        :history
        #(d/q '[:find ?cid ?cd ?cw ?district ?w ?amount ?data
                :with ?e
                :where [?e :history/c-id ?cid] [?e :history/c-d-id ?cd]
                       [?e :history/c-w-id ?cw] [?e :history/d-id ?district]
                       [?e :history/w-id ?w] [?e :history/amount ?amount]
                       [?e :history/data ?data]] (d/db conn))})
      (finally (d/close conn)))))

(defn- check-sql-payloads!
  [^Connection conn dialect new-order! payment!]
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
  (check-payloads!
   {:new-order! #(new-order! conn %)
    :payment! #(payment! conn %)
    :order-lines #(#'sqlite/qall conn
                               "SELECT ol_number, ol_i_id, ol_supply_w_id, ol_dist_info FROM order_line WHERE ol_w_id=2 AND ol_d_id=? AND ol_o_id=? ORDER BY ol_number"
                               %1 %2)
    :customer-data #(first (#'sqlite/q1 conn
                                       "SELECT c_data FROM customer WHERE c_w_id=2 AND c_d_id=? AND c_id=?"
                                       %1 %2))
    :history #(mapv (fn [row] (update row 5 double))
                    (#'sqlite/qall conn
                                   "SELECT h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_amount, h_data FROM history"))}))

(deftest sqlite-transaction-payloads
  (with-open [conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
    (check-sql-payloads! conn "sqlite" sqlite/new-order! sqlite/payment!)))

(deftest postgres-transaction-payloads
  (if-let [url (System/getenv "TPCC_TEST_PG_URL")]
    (with-open [conn (DriverManager/getConnection
                     url (or (System/getenv "TPCC_TEST_PG_USER") (System/getenv "USER"))
                     (or (System/getenv "TPCC_TEST_PG_PASS") ""))]
      (let [schema (str "tpcc_payload_" (str/replace (str (random-uuid)) "-" ""))]
        (#'sqlite/exec! conn (str "CREATE SCHEMA " schema))
        (try
          (#'sqlite/exec! conn (str "SET search_path TO " schema))
          (check-sql-payloads! conn "postgres" postgres/new-order! postgres/payment!)
          (finally
            (when-not (.getAutoCommit conn) (.rollback conn))
            (.setAutoCommit conn true)
            (#'sqlite/exec! conn (str "DROP SCHEMA " schema " CASCADE"))))))
    (println "Skipping PostgreSQL payload test; set TPCC_TEST_PG_URL to enable it.")))
