(ns datalevin-tpch.verify
  "Compare Datalevin Datalog results against the SQLite reference.

  Required ORDER BY is checked on the original results before canonicalizing
  rows for content comparison, allowing arbitrary order among ties. For a
  `:limit` query, the complete SQL result supplies every eligible boundary-tie
  row; different selections must still match real oracle rows and multiplicity.
  Singleton values, counts and quantity sums are exact. Other aggregates allow
  relative tolerance for floating-point rounding differences between backends."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin.core :as d]
   [datalevin-tpch.common :as c]
   [datalevin-tpch.queries :as q]
   [datalevin-tpch.sqlite :as sq])
  (:import
   [java.sql DriverManager]))

(defn- reference-sql
  "Keep the full ordered SQL result so LIMIT ties can be checked against real
  rows, including columns that are not part of the ordering key."
  [n]
  (let [sql (slurp (c/query-file :sqlite n))]
    (if (q/limit n)
      (str/replace sql #"(?i)\blimit\s+\d+\s*;?\s*$" ";")
      sql)))

(defn- sqlite-rows
  [^String db-file n]
  (with-open [conn (DriverManager/getConnection (str "jdbc:sqlite:" db-file))
              stmt (.createStatement conn)]
    (.execute stmt "PRAGMA case_sensitive_like=ON")
    (with-open [rs (.executeQuery stmt (reference-sql n))]
      (let [ncols (.getColumnCount (.getMetaData rs))]
        (loop [rows []]
          (if (.next rs)
            (recur (conj rows
                         (mapv (fn [i] (.getObject rs (int (inc i))))
                               (range ncols))))
            rows))))))

(defn- datalevin-rows
  ([db n] (datalevin-rows db n nil))
  ([db n timeout-ms]
   (let [query (cond-> (q/datalog n) timeout-ms (conj :timeout timeout-ms))
         res (d/q query db)]
     (if (coll? res) (mapv vec res) [[res]]))))

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

(defn- same-ordering-key?
  "Whether two rows share the same ORDER BY key. `:relative` ordering columns
  (computed monetary values) use the same rounding tolerance as the cell
  comparison; everything else must be exactly equal."
  [rules ordering a b]
  (every? true?
          (map (fn [[column _]]
                 (let [va (nth a column)
                       vb (nth b column)]
                   (case (rules column)
                     :exact    (if (and (number? va) (number? vb))
                                 (zero? (compare va vb))
                                 (= va vb))
                     :relative (c/close-enough? va vb))))
               ordering)))

(defn- rows-subset?
  "Match complete rows without reusing an oracle occurrence. Boundary sort-key
  equality alone cannot establish identity, projected values, or multiplicity."
  [rules expected actual]
  (loop [remaining (normalize rules expected) rows (seq (normalize rules actual))]
    (if-let [row (first rows)]
      (if-let [index (first (keep-indexed
                             (fn [i candidate]
                               (when (rows-match? rules [candidate] [row]) i)) remaining))]
        (recur (into (subvec remaining 0 index) (subvec remaining (inc index))) (next rows))
        false)
      true)))

(defn results-match?
  "Compare the complete ordered SQL oracle with the Datalevin result.

  Enforce output width, ordering and the query's row limit. At a truncated
  boundary, every returned row must match an actual oracle occurrence; only
  which eligible tied rows were selected is unspecified."
  [n expected actual]
  (let [rules (or (column-comparisons n)
                  (throw (ex-info "No result comparison rules for query" {:query n})))
        width (count rules)
        ordering (vec (or (q/ordering n) []))
        limit (q/limit n)
        expected-count (if limit (min limit (count expected)) (count expected))]
    (and (every? #(= width (count %)) expected)
         (every? #(= width (count %)) actual)
         (= expected-count (count actual))
         (ordered? n expected)
         (ordered? n actual)
         (if (and limit (= limit expected-count) (seq ordering))
           (let [boundary (nth expected (dec limit))
                 [prefix tail] (split-with #(not (same-ordering-key? rules ordering boundary %)) expected)
                 tied (take-while #(same-ordering-key? rules ordering boundary %) tail)
                 [actual-prefix actual-tied] (split-at (count prefix) actual)]
             (and (rows-match? rules (normalize rules prefix) (normalize rules actual-prefix))
                  (rows-subset? rules tied actual-tied)))
           (rows-match? rules (normalize rules expected) (normalize rules actual))))))

(defn- require-store! [file kind]
  (when-not (.exists (io/file file))
    (throw (ex-info (str "Missing " (name kind) " database: " file "; load the TPC-H data first")
                    {:database kind :path (str file)}))))

(defn- table-counts!
  "Check that neither side is an empty or differently loaded database. Each
  table's first attribute occurs once per loaded row, including partsupp."
  [db db-file]
  (with-open [conn (DriverManager/getConnection (str "jdbc:sqlite:" db-file))
              stmt (.createStatement conn)]
    (into (array-map)
      (for [table c/table-order
            :let [actual (d/count-datoms db nil (first (c/attrs table)) nil)
                  expected (with-open [rs (.executeQuery stmt (str "SELECT count(*) FROM " table))]
                             (.next rs) (.getLong rs 1))]]
        (do
          (when-not (and (pos? expected) (= expected actual))
            (throw (ex-info "TPC-H table row counts do not match"
                            {:table table :sqlite expected :datalevin actual})))
          [table expected])))))

(defn- verify-one [db db-file n dump? timeout-ms]
  (print (format "  q%-2d ... " n))
  (flush)
  (try
    (let [t0 (System/nanoTime)
          expected (sqlite-rows db-file n)
          t1 (System/nanoTime)
          actual (datalevin-rows db n timeout-ms)
          t2 (System/nanoTime)
          ok? (results-match? n expected actual)
          result {:query n :ok? ok? :reference-rows (count expected)
                  :expected-rows (if-let [limit (q/limit n)] (min limit (count expected)) (count expected))
                  :actual-rows (count actual)
                  :sqlite-ms (/ (- t1 t0) 1.0e6) :datalevin-ms (/ (- t2 t1) 1.0e6)}]
      (println (format "%s (Datalevin %.1f ms; SQLite %.1f ms)"
                       (if ok? "PASS" "FAIL") (:datalevin-ms result) (:sqlite-ms result)))
      (when-not (ordered? n expected) (println "    SQLite result violates ORDER BY"))
      (when-not (ordered? n actual) (println "    Datalevin result violates ORDER BY"))
      (when (and (not ok?) dump?)
        (println "    sqlite:   " (pr-str expected))
        (println "    datalevin:" (pr-str actual)))
      result)
    (catch Exception e
      (println "ERROR:" (.getMessage e))
      {:query n :ok? false :error (.getMessage e) :data (ex-data e)})))

(defn run-verification
  "Return a correctness report after closing both databases.

  :out optionally writes a progressively updated EDN report. Diagnostic query
  timings include preparation and consumption; they are not benchmark passes.
  :timeout-ms optionally applies the native query deadline to each translation."
  [{:keys [dir sqlite queries dump? out timeout-ms]
    :or {dir "db" sqlite sq/default-db-name dump? false}}]
  (let [db-file (.getPath (io/file c/base-dir sqlite))
        db-dir (.getPath (io/file c/base-dir dir))
        ids (if (or (nil? queries) (= queries :all)) (q/query-ids) (vec queries))
        _ (when-not (and (seq ids) (every? (set (q/query-ids)) ids)
                         (= (count ids) (count (set ids)))
                         (or (nil? timeout-ms) (pos-int? timeout-ms)))
            (throw (ex-info "Invalid verification query selection or timeout" {:queries queries :timeout-ms timeout-ms})))
        save! (fn [report]
                (when out (spit (io/file c/base-dir out) (str (pr-str report) "\n")))
                report)]
    (save! {:status :preflight :queries ids :results []})
    (require-store! (io/file db-dir "data.mdb") :datalevin)
    (require-store! db-file :sqlite)
    (let [conn (d/get-conn db-dir)]
      (try
        (let [db (d/db conn)
              counts (table-counts! db db-file)
              report {:status :running :queries ids :table-counts counts
                      :java-version (System/getProperty "java.version")
                      :clojure-version (clojure-version) :results []}
              result (reduce (fn [report n]
                               (save! (update report :results conj (verify-one db db-file n dump? timeout-ms))))
                             (save! report) ids)
              passed (count (filter :ok? (:results result)))]
          (println (format "\n%d/%d queries passed" passed (count ids)))
          (save! (assoc result :status :complete :ok? (= passed (count ids)) :passed passed)))
        (finally (d/close conn))))))

(defn verify
  "CLI correctness gate. Options: :dir, :sqlite, :queries (:all by default),
  :dump?, :out (EDN report), and :timeout-ms (native per-query deadline)."
  [options]
  (let [result (run-verification options)]
    (shutdown-agents)
    (System/exit (if (:ok? result) 0 1))))
