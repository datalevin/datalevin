(ns datalevin-tpcc.stock-test
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
    (let [dir (u/tmp-dir (str "datalevin-tpcc-stock-" (random-uuid)))]
      (try
        (binding [*test-dir* dir] (f))
        (finally
          (when (.exists (u/file dir))
            (u/delete-files dir)))))))

(def initial-stock
  {[1 1] [10 20 30 40]
   [1 2] [15 22 32 42]
   [2 1] [14 24 34 44]})

(defn- line [w i qty]
  {:supply-w w :i-id i :qty qty})

(def stock-cases
  (concat
   (for [[stock expected] [[10 96] [14 100] [15 10] [16 11]]]
     {:label (str "stock " stock ", quantity 5")
      :stock (assoc-in initial-stock [[1 1] 0] stock)
      :ol [(line 1 1 5)]
      :expected (assoc initial-stock [1 1] [expected 25 31 40])})
   [{:label "repeated local item replenishes twice"
     :stock initial-stock
     :ol (vec (repeat 15 (line 1 1 10)))
     :expected (assoc initial-stock [1 1] [42 170 45 40])}
    {:label "repeated remote item increments remote count for every line"
     :stock initial-stock
     :ol (vec (repeat 15 (line 2 1 10)))
     :expected (assoc initial-stock [2 1] [46 174 49 59])}
    {:label "interleaved items and supplying warehouses stay independent"
     :stock initial-stock
     :ol [(line 1 1 5) (line 2 1 5) (line 1 2 5)
          (line 1 1 10) (line 2 1 10)]
     :expected {[1 1] [86 35 32 40]
                [1 2] [10 27 33 42]
                [2 1] [90 39 36 46]}}]))

(deftest stock-quantity-boundaries-and-multiple-replenishments
  ;; For legal stock and order-line quantities, replenishment wraps the stock
  ;; into [10, 100]. This modular oracle is independent of the per-line branch.
  (doseq [quantity (range 10 101)
          ordered (range 1 11)]
    (is (= [(+ 10 (mod (- quantity ordered 10) 91)) (+ 20 ordered) 31 40]
           (c/stock-after-lines [quantity 20 30 40] [ordered] false))))
  (doseq [quantity (range 10 101)
          n (range 1 16)
          remote? [false true]]
    (is (= [(+ 10 (mod (- quantity (* n 10) 10) 91))
            (+ 20 (* n 10)) (+ 30 n) (+ 40 (if remote? n 0))]
           (c/stock-after-lines [quantity 20 30 40] (repeat n 10) remote?)))))

(defn- seed-rows []
  (let [stock (first (g/stock-rows 42 1))]
    {"warehouse" (g/warehouse-rows 42 2)
     "district" (take 1 (g/district-rows 42 1))
     "customer" (take 1 (g/customer-rows 42 1))
     "item" (take 2 (g/item-rows 42))
     "stock" (for [[[w i] [qty ytd ocnt rcnt]] initial-stock]
               (assoc stock 0 i 1 w 2 qty 13 ytd 14 ocnt 15 rcnt))}))

(defn- check-new-orders!
  [{:keys [reset-stock! new-order! stocks order-lines order-count]}]
  (doseq [{:keys [label stock ol expected]} stock-cases]
    (testing label
      (reset-stock! stock)
      (let [result (new-order! {:w 1 :d 1 :c 1 :ol ol})]
        (is (= :ok (:status result)))
        (is (= expected (stocks)))
        (is (= (mapv (fn [n {:keys [i-id supply-w qty]}]
                       [(inc n) i-id supply-w qty])
                     (range) ol)
               (order-lines (:o-id result)))))))
  (testing "an invalid item rolls back repeated-item stock and order changes"
    (reset-stock! initial-stock)
    (let [before (order-count)
          result (new-order! {:w 1 :d 1 :c 1
                              :ol (conj (vec (repeat 14 (line 1 1 10)))
                                        (line 1 999 1))})]
      (is (= :invalid-item (:status result)))
      (is (= initial-stock (stocks)))
      (is (= before (order-count))))))

(deftest datalevin-stock-updates
  (let [conn (d/get-conn *test-dir* datalevin/schema)]
    (try
      (d/transact! conn
                   (vec (for [[table rows] (seed-rows) row rows]
                          (zipmap (c/attrs table) row))))
      (let [eids (into {} (map (fn [[e w i]] [[w i] e]))
                       (d/q '[:find ?e ?w ?i
                              :where [?e :stock/w-id ?w] [?e :stock/i-id ?i]]
                            (d/db conn)))]
        (check-new-orders!
         {:reset-stock!
          (fn [stocks]
            (d/transact! conn
                         (mapv (fn [[k [qty ytd ocnt rcnt]]]
                                 {:db/id (eids k) :stock/quantity qty :stock/ytd ytd
                                  :stock/order-cnt ocnt :stock/remote-cnt rcnt})
                               stocks)))
          :new-order! #(t/new-order! conn %)
          :stocks
          #(into {} (map (fn [[w i & values]] [[w i] (vec values)]))
                 (d/q '[:find ?w ?i ?qty ?ytd ?ocnt ?rcnt
                        :where [?e :stock/w-id ?w] [?e :stock/i-id ?i]
                               [?e :stock/quantity ?qty] [?e :stock/ytd ?ytd]
                               [?e :stock/order-cnt ?ocnt] [?e :stock/remote-cnt ?rcnt]]
                      (d/db conn)))
          :order-lines
          (fn [o-id]
            (vec (sort (d/q '[:find ?n ?i ?w ?qty :in $ ?o
                              :where [?e :order-line/o-id ?o]
                                     [?e :order-line/number ?n]
                                     [?e :order-line/i-id ?i]
                                     [?e :order-line/supply-w-id ?w]
                                     [?e :order-line/quantity ?qty]]
                            (d/db conn) o-id))))
          :order-count #(t/order-count conn 1 1)}))
      (finally (d/close conn)))))

(defn- sql-exec! [^Connection conn sql params]
  (with-open [ps (.prepareStatement conn sql)]
    (doseq [[i v] (map-indexed vector params)]
      (.setObject ps (int (inc i)) v))
    (.executeUpdate ps)))

(defn- sql-rows [^Connection conn sql params]
  (with-open [ps (.prepareStatement conn sql)]
    (doseq [[i v] (map-indexed vector params)]
      (.setObject ps (int (inc i)) v))
    (with-open [rs (.executeQuery ps)]
      (let [n (.getColumnCount (.getMetaData rs))]
        (loop [rows []]
          (if (.next rs)
            (recur (conj rows (mapv #(.getObject rs (int %)) (range 1 (inc n)))))
            rows))))))

(defn- check-sql-stock! [^Connection conn dialect new-order!]
  (with-open [st (.createStatement conn)]
    (doseq [sql (-> (slurp (io/file (c/data-dir) (str "schema-" dialect ".sql")))
                   (str/replace #"(?m)--[^\n]*" "")
                   (str/split #";"))
            :when (not (str/blank? sql))]
      (.execute st sql)))
  (doseq [[table rows] (seed-rows)
          :let [columns (:columns (c/table-specs table))
                sql (str "INSERT INTO " table " (" (str/join "," columns) ") VALUES ("
                         (str/join "," (repeat (count columns) "?")) ")")]
          row rows]
    (sql-exec! conn sql row))
  (check-new-orders!
   {:reset-stock!
    (fn [stocks]
      (.setAutoCommit conn true)
      (doseq [[[w i] values] stocks]
        (sql-exec! conn
                   "UPDATE stock SET s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? WHERE s_w_id=? AND s_i_id=?"
                   (into values [w i]))))
    :new-order! #(new-order! conn %)
    :stocks #(into {} (map (fn [[w i & values]] [[w i] (vec values)]))
                   (sql-rows conn
                             "SELECT s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt FROM stock"
                             []))
    :order-lines #(sql-rows conn
                           "SELECT ol_number, ol_i_id, ol_supply_w_id, ol_quantity FROM order_line WHERE ol_o_id=? ORDER BY ol_number"
                           [%])
    :order-count #(ffirst (sql-rows conn "SELECT count(*) FROM orders" []))}))

(deftest sqlite-stock-updates
  (with-open [conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
    (check-sql-stock! conn "sqlite" sqlite/new-order!)))

(deftest postgres-stock-updates
  (if-let [url (System/getenv "TPCC_TEST_PG_URL")]
    (with-open [conn (DriverManager/getConnection
                     url (or (System/getenv "TPCC_TEST_PG_USER") (System/getenv "USER"))
                     (or (System/getenv "TPCC_TEST_PG_PASS") ""))]
      (let [schema (str "tpcc_stock_" (str/replace (str (random-uuid)) "-" ""))]
        (sql-exec! conn (str "CREATE SCHEMA " schema) [])
        (try
          (sql-exec! conn (str "SET search_path TO " schema) [])
          (check-sql-stock! conn "postgres" postgres/new-order!)
          (finally
            (when-not (.getAutoCommit conn) (.rollback conn))
            (.setAutoCommit conn true)
            (sql-exec! conn (str "DROP SCHEMA " schema " CASCADE") [])))))
    (println "Skipping PostgreSQL stock integration test; set TPCC_TEST_PG_URL to enable it.")))
