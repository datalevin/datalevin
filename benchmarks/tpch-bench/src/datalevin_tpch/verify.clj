(ns datalevin-tpch.verify
  "Compare Datalevin Datalog results against the SQLite reference.

  Required ORDER BY is checked on the original results before canonicalizing
  rows for content comparison, allowing arbitrary order among ties. Singleton
  values, counts and quantity sums are exact. Other aggregates allow relative
  tolerance for floating-point rounding differences between backends."
  (:require
   [clojure.java.io :as io]
   [datalevin.core :as d]
   [datalevin-tpch.common :as c]
   [datalevin-tpch.queries :as q]
   [datalevin-tpch.sqlite :as sq])
  (:import
   [java.sql DriverManager]))

(defn- sqlite-rows
  [^String db-file n]
  (with-open [conn (DriverManager/getConnection (str "jdbc:sqlite:" db-file))
              stmt (.createStatement conn)]
    (.execute stmt "PRAGMA case_sensitive_like=ON")
    (with-open [rs (.executeQuery stmt (slurp (c/query-file :sqlite n)))]
      (let [ncols (.getColumnCount (.getMetaData rs))]
        (loop [rows []]
          (if (.next rs)
            (recur (conj rows
                         (mapv (fn [i] (.getObject rs (int (inc i))))
                               (range ncols))))
            rows))))))

(defn- datalevin-rows
  [db n]
  (let [res (d/q (q/datalog n) db)]
    (cond
      (coll? res)  (mapv vec res)
      :else        [[res]])))

(def ^:private column-comparisons
  "Comparison rules in SELECT-list order, per TPC-H 2.1.3.5. Only computed
  monetary aggregates, averages and ratios allow rounding tolerance. Singleton
  values (including balances/prices), COUNTs, Q12's conditional counts and
  Q1/Q18's SUM(l_quantity) require exact equality."
  {1  [:exact :exact :exact :relative :relative :relative :relative :relative :relative :exact]
   2  [:exact :exact :exact :exact :exact :exact :exact :exact]
   3  [:exact :relative :exact :exact]
   4  [:exact :exact]
   5  [:exact :relative]
   6  [:relative]
   7  [:exact :exact :exact :relative]
   8  [:exact :relative]
   9  [:exact :exact :relative]
   10 [:exact :exact :relative :exact :exact :exact :exact :exact]
   11 [:exact :relative]
   12 [:exact :exact :exact]
   13 [:exact :exact]
   14 [:relative]
   15 [:exact :exact :exact :exact :relative]
   16 [:exact :exact :exact :exact]
   17 [:relative]
   18 [:exact :exact :exact :exact :exact :exact]
   19 [:relative]
   20 [:exact :exact]
   21 [:exact :exact]
   22 [:exact :exact :relative]})

(defn- normalize
  [rules rows]
  ;; Compare decimal representations across JDBC/Clojure numeric types without
  ;; rounding integer keys to doubles. Sort exact columns first so rounding in
  ;; aggregates cannot change which identifiers are paired for comparison.
  (let [columns (sort-by #(if (= :exact (rules %)) 0 1) (range (count rules)))]
    (->> rows
         (map (fn [row]
                (mapv (fn [v] (if (number? v) (bigdec v) v)) row)))
         (sort-by #(mapv % columns))
         vec)))

(defn- rows-match?
  [rules a b]
  (and (= (count a) (count b))
       (every? true?
               (map (fn [ra rb]
                      (every? true?
                              (map (fn [rule va vb]
                                     (case rule
                                       :exact (= va vb)
                                       :relative (c/close-enough? va vb)))
                                   rules ra rb)))
                    a b))))

(defn ordered?
  "Whether rows already satisfy query `n`'s ORDER BY. Equal keys may appear
  in any order; unordered queries impose no ordering requirement."
  [n rows]
  (let [ordering (q/ordering n)
        compare-rows
        (fn [a b]
          (loop [[[column direction] & more] ordering]
            (if (some? column)
              (let [cmp (if (= direction :desc)
                          (compare (nth b column) (nth a column))
                          (compare (nth a column) (nth b column)))]
                (if (zero? cmp) (recur more) cmp))
              0)))]
    (every? (fn [[a b]] (not (pos? (compare-rows a b))))
            (partition 2 1 rows))))

(defn results-match?
  "Check row shape and required ordering, then compare each output column using
  its exact or aggregate-rounding rule."
  [n expected actual]
  (let [rules (or (column-comparisons n)
                  (throw (ex-info "No result comparison rules for query"
                                  {:query n})))
        width (count rules)]
    (and (every? #(= width (count %)) expected)
         (every? #(= width (count %)) actual)
         (ordered? n expected)
         (ordered? n actual)
         (rows-match? rules (normalize rules expected) (normalize rules actual)))))

(defn verify
  "Run the Datalevin translations and check them against SQLite.

  Options:
    :dir      Datalevin database directory (default \"db\")
    :sqlite   SQLite database file (default \"sqlite.db\")
    :queries  vector of query numbers, or :all (default :all)
    :dump?    print both original result sets for mismatches (default false)"
  [{:keys [dir sqlite queries dump?]
    :or   {dir "db" sqlite sq/default-db-name dump? false}}]
  (let [db-file (.getPath (io/file c/base-dir sqlite))
        ids     (if (or (nil? queries) (= queries :all))
                  (q/query-ids)
                  (mapv long queries))
        conn    (d/get-conn (.getPath (io/file c/base-dir dir)))]
    (try
      (let [results
            (doall
             (for [n ids]
               (do
                 (print (format "  q%-2d ... " n)) (flush)
                 (try
                   (let [expected (sqlite-rows db-file n)
                         actual   (datalevin-rows (d/db conn) n)
                         ok?      (results-match? n expected actual)]
                     (println (if ok? "PASS" "FAIL"))
                     (when-not (ordered? n expected)
                       (println "    SQLite result violates ORDER BY"))
                     (when-not (ordered? n actual)
                       (println "    Datalevin result violates ORDER BY"))
                     (when (and (not ok?) dump?)
                       (println "    sqlite:   " (pr-str expected))
                       (println "    datalevin:" (pr-str actual)))
                     {:query n :ok? ok?
                      :expected-rows (count expected)
                      :actual-rows (count actual)})
                   (catch Throwable e
                     (println "ERROR:" (.getMessage e))
                     {:query n :ok? false :error (.getMessage e)})))))
            failed (remove :ok? results)]
        (println)
        (println (format "%d/%d queries passed" (- (count results) (count failed))
                         (count results)))
        (when (seq failed)
          (println "Failed:" (mapv :query failed))
          (System/exit 1)))
      (finally
        (d/close conn))))
  (shutdown-agents)
  (System/exit 0))
