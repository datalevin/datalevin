(ns datalevin-tpch.ordering-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [clojure.test :refer [deftest is testing]]
   [datalevin.core :as d]
   [datalevin.parser :as parser]
   [datalevin-tpch.common :as c]
   [datalevin-tpch.datalevin :as dt]
   [datalevin-tpch.queries :as q]
   [datalevin-tpch.sqlite :as sq]
   [datalevin-tpch.verify :as v])
  (:import
   [java.sql Connection DriverManager ResultSet]
   [java.util Random]))

(defn- sql-ordering
  [columns sql]
  (when-let [clause (second (re-find #"(?is)\border\s+by\s+(.+?)(?:\s+limit\s+\d+)?;?\s*$" sql))]
    (mapv (fn [term]
            (let [[column direction] (s/split (s/trim term) #"\s+")]
              [(.indexOf ^java.util.List columns column)
               (keyword (or direction "asc"))]))
          (s/split clause #","))))

(defn- result-rows
  [^ResultSet rs]
  (let [ncols (.getColumnCount (.getMetaData rs))]
    (loop [rows []]
      (if (.next rs)
        (recur (conj rows (mapv #(.getObject rs (int %)) (range 1 (inc ncols)))))
        rows))))

(defn- sorted-fixture
  "Ask SQLite to sort projected rows using the ORDER BY read from its query."
  [^Connection conn rows ordering]
  (let [columns (mapv #(str "col" %) (range (count (first rows))))
        sql (str "WITH fixture(" (s/join "," columns) ") AS (VALUES "
                 (s/join "," (map #(str "(" (s/join "," %) ")") rows))
                 ") SELECT * FROM fixture ORDER BY "
                 (s/join "," (map (fn [[column direction]]
                                    (str (columns column) " " (name direction)))
                                  ordering)))]
    (with-open [stmt (.createStatement conn)
                rs (.executeQuery stmt sql)]
      (result-rows rs))))

(deftest all-query-ordering-matches-sql
  (with-open [conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
    (#'sq/exec-sql-file! conn (io/file (c/data-dir) "schema-sqlite.sql"))
    (doseq [n (q/query-ids)]
      (testing (str "Q" n)
        (with-open [stmt (.createStatement conn)
                    rs (.executeQuery stmt (slurp (c/query-file :sqlite n)))]
          (let [metadata (.getMetaData rs)
                width (.getColumnCount metadata)
                columns (mapv #(.getColumnLabel metadata (int %))
                              (range 1 (inc width)))
                ordering (sql-ordering columns (slurp (c/query-file :sqlite n)))
                parsed (parser/parse-query (q/datalog n))]
            (doseq [dialect [:sqlite :postgres]]
              (let [sql-order (sql-ordering columns (slurp (c/query-file dialect n)))]
                (is (every? (comp nat-int? first) sql-order)
                    (str dialect " sort columns exist in the output"))
                (is (= (seq sql-order) (q/ordering n)) (name dialect))))
            (is (= (seq ordering) (seq (partition 2 (:qorder parsed))))
                "the native query parser sees the required ordering")
            (when (seq ordering)
              (let [rng (Random. n)
                    rows (vec (distinct (repeatedly 100
                                          #(mapv (fn [_] (.nextInt rng 3))
                                                 (range width)))))
                    vars (mapv #(symbol (str "?col" %)) (range width))
                    ;; Exercise native ordering over the final projection,
                    ;; independently of each query's joins and aggregates.
                    query (vec (concat [:find] vars
                                       [:in [vars] :order-by (vec (mapcat identity
                                                                         (q/ordering n)))]))
                    expected (sorted-fixture conn rows ordering)
                    actual (d/q query rows)]
                (is (v/results-match? n expected actual))
                (is (= (set rows) (set actual)) "sorting retains every row")))))))))

(deftest verification-checks-original-order
  (let [a [10 30.0 "1995-03-01" 0]
        b [20 20.0 "1995-03-01" 0]
        c [30 20.0 "1995-03-02" 0]
        tied [40 20.0 "1995-03-02" 0]
        rows [a b c tied]]
    (is (v/results-match? 3 rows rows))
    (is (not (v/results-match? 3 rows [b a c tied])) "descending revenue")
    (is (not (v/results-match? 3 rows [a c b tied])) "ascending date breaks revenue ties")
    (is (not (v/results-match? 3 (reverse rows) rows)) "check the oracle's ordering too")
    (is (v/results-match? 3 rows [a b tied c]) "SQL leaves complete ties unordered")
    (is (v/results-match? 3 rows (assoc-in rows [0 1] 30.0000001)) "numeric tolerance")
    (is (not (v/results-match? 3 rows (assoc-in rows [0 1] 31.0))))
    (is (not (v/results-match? 3 rows (conj rows tied))) "retain multiplicity")
    (is (v/results-match? 3 [] []))
    (is (v/results-match? 6 [[nil]] [[nil]]) "scalar SQL NULL")
    (is (v/results-match? 14 [[1] [2]] [[2] [1]]) "no ORDER BY means no order check")))

(def q3-orders
  ;; Insertion and order-key order both disagree with the required revenue/date
  ;; order. Equal-price lineitems must still contribute twice to each sum.
  [[10 "1995-03-02" 10.0]
   [20 "1995-03-01" 15.0]
   [30 "1995-03-01" 10.0]])

(defn- q3-datoms
  ([] (q3-datoms q3-orders))
  ([orders]
   (let [entities
         (into [{:customer/custkey 1 :customer/mktsegment "BUILDING"}]
               (mapcat (fn [[orderkey date price]]
                         (let [line {:lineitem/orderkey orderkey
                                     :lineitem/shipdate "1995-03-20"
                                     :lineitem/extendedprice price
                                     :lineitem/discount 0.0}]
                           [{:orders/orderkey orderkey :orders/custkey 1
                             :orders/orderdate date :orders/shippriority 0}
                            line line]))
                       orders))]
     (vec (mapcat (fn [eid entity]
                    (map (fn [[a value]] [eid a value]) entity))
                  (range 1 (inc (count entities))) entities)))))

(deftest q3-orders-aggregates-inside-the-query
  (let [datoms (q3-datoms)
        expected [[20 30.0 "1995-03-01" 0]
                  [30 20.0 "1995-03-01" 0]
                  [10 20.0 "1995-03-02" 0]]]
    (is (= expected (d/q q/q-3 datoms)))
    (doseq [explain? [false true]]
      (let [{:keys [rows prepare-ms exec-ms wall-ms]}
            (#'dt/run-one datoms 3 explain?)]
        (is (= 3 rows))
        (is (pos? wall-ms))
        (when explain?
          (is (some? exec-ms))
          (is (< (abs (- wall-ms (+ (parse-double prepare-ms)
                                   (parse-double exec-ms))))
                 0.001)
              "reported execution includes all time after preparation"))))))

(deftest q3-row-limit-truncates-to-ten
  ;; TPC-H 2.1.2.9 limits Q3 to 10 rows. Eleven qualifying orders must still
  ;; return ten, highest revenue first.
  (let [orders (vec (for [i (range 1 12)]
                      [(+ 100 i) "1995-03-01" (double (- 100 i))]))
        rows   (d/q q/q-3 (q3-datoms orders))]
    (is (= 10 (count rows)))
    (is (= (mapv #(+ 100 %) (range 1 11)) (mapv first rows)))))
