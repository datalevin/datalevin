(ns datalevin-tpch.verify
  "Compare Datalevin Datalog results against the SQLite reference.

  TPC-H result sets are unordered unless the query specifies ORDER BY, so both
  sides are normalized to a canonical order before comparison. Numeric cells are
  compared with a relative tolerance because SQLite stores money as REAL while
  PostgreSQL and Datalevin keep exact or double values."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
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
    (let [rs (.executeQuery stmt (slurp (c/query-file :sqlite n)))]
      (loop [rows []]
        (if (.next rs)
          (recur (conj rows
                       (mapv (fn [i] (.getObject rs (int (inc i))))
                             (range (.getColumnCount rs)))))
          rows)))))

(defn- datalevin-rows
  [db n]
  (let [res (d/q (q/datalog n) db)]
    (cond
      (coll? res)  (mapv vec res)
      :else        [[res]])))

(defn- normalize
  [rows]
  (->> rows
       (map (fn [row]
              (mapv (fn [v] (if (number? v) (double v) v)) row)))
       (sort-by pr-str)
       vec))

(defn- rows-match?
  [a b]
  (and (= (count a) (count b))
       (every? true?
               (map (fn [ra rb]
                      (and (= (count ra) (count rb))
                           (every? true?
                                   (map c/close-enough? ra rb))))
                    a b))))

(defn verify
  "Run the Datalevin translations and check them against SQLite.

  Options:
    :dir      Datalevin database directory (default \"db\")
    :sqlite   SQLite database file (default \"sqlite.db\")
    :queries  vector of query numbers, or :all (default :all)
    :dump?    print both normalized result sets for mismatches (default false)"
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
                   (let [expected (normalize (sqlite-rows db-file n))
                         actual   (normalize (datalevin-rows (d/db conn) n))
                         ok?      (rows-match? expected actual)]
                     (println (if ok? "PASS" "FAIL"))
                     (when (and (not ok?) dump?)
                       (println "    sqlite:   " (pr-str expected))
                       (println "    datalevin:" (pr-str actual)))
                     {:query n :ok? ok?
                      :expected-rows (count expected)
                      :actual-rows (count actual)})
                   (catch Throwable e
                     (println "ERROR:" (.getMessage e))
                     {:query n :ok? false :error (.getMessage e)})))))]
        (let [failed (remove :ok? results)]
          (println)
          (println (format "%d/%d queries passed" (- (count results) (count failed))
                           (count results)))
          (when (seq failed)
            (println "Failed:" (mapv :query failed))
            (System/exit 1))))
      (finally
        (d/close conn))))
  (shutdown-agents)
  (System/exit 0))
