(ns datalevin-bench.cardinality-sql
  "Exact JOB subset counts through the workload's equivalent SQL relations.

  This is an offline oracle-construction backend, not a runtime baseline. Every
  pre-existing Datalevin count is checked before SQL fills missing checkpoints."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [datalevin-bench.cardinality-oracle :as oracle]
   [datalevin-bench.core :as job]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu])
  (:import
   [java.sql Connection DriverManager Statement]
   [java.util BitSet]
   [java.util.concurrent Callable ExecutorService Executors Future]))

(def table-bases
  {"aka_name"        job/aka-name-base
   "aka_title"       job/aka-title-base
   "cast_info"       job/cast-info-base
   "char_name"       job/char-name-base
   "comp_cast_type"  job/comp-cast-type-base
   "company_name"    job/company-name-base
   "company_type"    job/company-type-base
   "complete_cast"   job/complete-cast-base
   "info_type"       job/info-type-base
   "keyword"         job/keyword-base
   "kind_type"       job/kind-type-base
   "link_type"       job/link-type-base
   "movie_companies" job/movie-companies-base
   "movie_info"      job/movie-info-base
   "movie_info_idx"  job/movie-info-idx-base
   "movie_keyword"   job/movie-keyword-base
   "movie_link"      job/movie-link-base
   "name"            job/name-base
   "person_info"     job/person-info-base
   "role_type"       job/role-type-base
   "title"           job/title-base})

(defn- word-char?
  [c]
  (or (Character/isLetterOrDigit c) (= c \_)))

(defn- word-at?
  [^String s ^long i ^String word]
  (let [n (.length s)
        w (.length word)
        end (+ i w)]
    (and (<= end n)
         (.regionMatches s true (int i) word 0 w)
         (or (zero? i) (not (word-char? (.charAt s (dec i)))))
         (or (= end n) (not (word-char? (.charAt s end)))))))

(defn split-top-level-and
  "Split a SQL WHERE expression on top-level AND, preserving BETWEEN ... AND
  and quoted strings."
  [where]
  (let [^String where (str/trim (str/replace where #";\s*$" ""))
        n (.length where)]
    (loop [i             0
           start         0
           depth         0
           quoted?       false
           between-depth nil
           result        []]
      (if (>= i n)
        (cond-> result
          (< start n) (conj (str/trim (subs where start n))))
        (let [ch (.charAt where i)]
          (cond
            quoted?
            (if (= ch \')
              (if (and (< (inc i) n) (= (.charAt where (inc i)) \'))
                (recur (+ i 2) start depth true between-depth result)
                (recur (inc i) start depth false between-depth result))
              (recur (inc i) start depth true between-depth result))

            (= ch \')
            (recur (inc i) start depth true between-depth result)

            (= ch \()
            (recur (inc i) start (inc depth) false between-depth result)

            (= ch \))
            (recur (inc i) start (dec depth) false between-depth result)

            (word-at? where i "BETWEEN")
            (recur (+ i 7) start depth false depth result)

            (word-at? where i "AND")
            (if (= between-depth depth)
              (recur (+ i 3) start depth false nil result)
              (if (zero? depth)
                (recur (+ i 3) (+ i 3) depth false between-depth
                       (conj result (str/trim (subs where start i))))
                (recur (+ i 3) start depth false between-depth result)))

            :else
            (recur (inc i) start depth false between-depth result)))))))

(defn- parse-from-item
  [item]
  (let [item (str/trim item)]
    (or
      (when-let [[_ table alias]
                 (re-matches
                   #"(?i)([a-z][a-z0-9_]*)\s+(?:AS\s+)?([a-z][a-z0-9_]*)"
                   item)]
        {:table (str/lower-case table)
         :alias (str/lower-case alias)
         :sql item})
      (throw (ex-info "Cannot parse JOB SQL FROM item" {:item item})))))

(defn parse-job-sql
  [sql]
  (let [[_ from where]
        (or (re-find #"(?is)\bFROM\s+(.*?)\s+\bWHERE\b\s+(.*)$" sql)
            (throw (ex-info "Cannot find FROM/WHERE in JOB SQL" {})))
        from-items (mapv parse-from-item (str/split from #","))
        conditions
        (mapv
          (fn [condition]
            {:sql condition
             :aliases
             (into #{}
                   (map (comp str/lower-case second))
                   (re-seq
                     #"(?i)(?:^|[^a-z0-9_])([a-z][a-z0-9_]*)\."
                     condition))})
          (split-top-level-and where))]
    {:from-items from-items
     :conditions conditions}))

(defn entity-alias
  [entity]
  (let [raw (name entity)
        raw (if (str/starts-with? raw "?placeholder__")
              (subs raw (count "?placeholder__"))
              (subs raw 1))]
    (str/replace raw "-" "_")))

(defn subset-sql
  ([sql-spec entities]
   (subset-sql sql-spec entities {} {} {}))
  ([sql-spec entities required-columns]
   (subset-sql sql-spec entities required-columns {} {}))
  ([{:keys [from-items conditions]} entities required-columns excluded-ids]
   (subset-sql {:from-items from-items :conditions conditions}
               entities required-columns excluded-ids {}))
  ([{:keys [from-items conditions]} entities required-columns excluded-ids
    join-columns]
   (let [aliases (into #{} (map entity-alias) entities)
         selected-from (filterv #(contains? aliases (:alias %)) from-items)
         selected-conditions
         (filterv #(set/subset? (:aliases %) aliases) conditions)
         existence-conditions
         (for [alias (sort aliases)
               column (sort (get required-columns alias))]
           (str alias "." column " IS NOT NULL"))
         exclusion-conditions
         (for [alias (sort aliases)
               :let [ids (get excluded-ids alias)]
               :when (seq ids)]
           (str alias ".id NOT IN (" (str/join "," ids) ")"))
         join-conditions
         (mapcat
           (fn [[_var occurrences]]
             (let [selected (->> occurrences
                                 (filter #(contains? aliases (first %)))
                                 distinct
                                 (sort-by pr-str))]
               (when (< 1 (count selected))
                 (let [[left-alias left-column] (first selected)]
                   (for [[right-alias right-column] (next selected)]
                     (str left-alias "." left-column " = "
                          right-alias "." right-column))))))
           (sort-by (comp pr-str key) join-columns))
         where-conditions
         (concat (map :sql selected-conditions)
                 existence-conditions exclusion-conditions join-conditions)]
     (when-not (= aliases (into #{} (map :alias) selected-from))
       (throw (ex-info "Datalog entity aliases do not match JOB SQL"
                       {:entities entities
                        :aliases aliases
                        :sql-aliases (mapv :alias from-items)})))
     (str "SELECT COUNT(*) FROM "
          (str/join ", " (map :sql selected-from))
          (when (seq where-conditions)
            (str " WHERE " (str/join " AND " where-conditions)))))))

(defn factorized-subset-sql
  "Build an exact weighted join count using variable elimination. Each
  relation is first grouped by only the variables shared with another selected
  relation. Leaf variables are then summed out before their factors are joined
  to the rest of the query, avoiding materialization of the full result."
  [{:keys [from-items conditions prepared-factors]} entities required-columns
   excluded-ids join-columns]
  (let [aliases       (into #{} (map entity-alias) entities)
        selected-from (filterv #(contains? aliases (:alias %)) from-items)
        alias-order   (mapv :alias selected-from)
        alias-index   (zipmap alias-order (range))
        selected-occurrences
        (into {}
              (map (fn [[var occurrences]]
                     [var (->> occurrences
                               (filter #(contains? aliases (first %)))
                               distinct
                               (sort-by pr-str)
                               vec)]))
              join-columns)
        outer-vars
        (->> selected-occurrences
             (filter (fn [[_ occurrences]]
                       (< 1 (count (distinct (map first occurrences))))))
             (map key)
             (sort-by pr-str)
             vec)
        var-name      (or (:var-name prepared-factors)
                          (into {} (map-indexed (fn [i var]
                                                 [var (str "v" i)]))
                                outer-vars))
        local-equalities
        (reduce-kv
          (fn [result _var occurrences]
            (reduce
              (fn [result [_alias same-alias]]
                (if (< 1 (count same-alias))
                  (let [[left-alias left-column] (first same-alias)]
                    (into result
                          (for [[right-alias right-column]
                                (next same-alias)]
                            (str left-alias "." left-column " = "
                                 right-alias "." right-column))))
                  result))
              result (group-by first occurrences)))
          [] selected-occurrences)
        initial-factors
        (mapv
          (fn [{:keys [alias sql]}]
            (let [columns
                  (mapv
                    (fn [var]
                      [var (second
                             (first
                               (filter #(= alias (first %))
                                       (selected-occurrences var))))])
                    (filter
                      (fn [var]
                        (some #(= alias (first %))
                              (selected-occurrences var)))
                      outer-vars))
                  local-conditions
                  (concat
                    (map :sql
                         (filter #(or (empty? (:aliases %))
                                      (= #{alias} (:aliases %)))
                                 conditions))
                    (for [column (sort (get required-columns alias))]
                      (str alias "." column " IS NOT NULL"))
                    (when-let [ids (seq (get excluded-ids alias))]
                      [(str alias ".id NOT IN (" (str/join "," ids) ")")])
                    (filter #(str/starts-with? % (str alias "."))
                            local-equalities))
                  select-columns
                  (map (fn [[var column]]
                         (str alias "." column " AS " (var-name var)))
                       columns)
                  group-columns
                  (map (fn [[_var column]] (str alias "." column)) columns)
                  needed-vars (into #{} (map first) columns)]
              (if prepared-factors
                (let [{:keys [vars]}
                      (or (get-in prepared-factors [:factors alias])
                          (throw (ex-info "Prepared SQL factor is missing"
                                          {:alias alias})))
                      needed-columns (mapv var-name
                                           (sort-by pr-str needed-vars))
                      projection-table
                      (or (get-in prepared-factors
                                  [:factors alias :projections needed-vars])
                          (throw
                            (ex-info "Prepared SQL projection is missing"
                                     {:alias alias
                                      :needed-vars needed-vars
                                      :factor-vars vars})))]
                  {:name (str "f" (alias-index alias))
                   :vars needed-vars
                   :sql  (str "SELECT "
                              (when (seq needed-columns)
                                (str (str/join ", " needed-columns) ", "))
                              "w AS w FROM " projection-table)})
                {:name (str "f" (alias-index alias))
                 :vars needed-vars
                 :sql  (str "SELECT "
                            (when (seq select-columns)
                              (str (str/join ", " select-columns) ", "))
                            "COUNT(*)::numeric AS w FROM " sql
                            (when (seq local-conditions)
                              (str " WHERE "
                                   (str/join " AND " local-conditions)))
                            (when (seq group-columns)
                              (str " GROUP BY "
                                   (str/join ", " group-columns))))})))
          selected-from)
        var-order (zipmap outer-vars (range))]
    (when-not (= aliases (set alias-order))
      (throw (ex-info "Datalog entity aliases do not match JOB SQL"
                      {:entities entities
                       :aliases aliases
                       :sql-aliases (mapv :alias from-items)})))
    (loop [factors   (mapv #(select-keys % [:name :vars]) initial-factors)
           variables (set outer-vars)
           ctes      (mapv (fn [{:keys [name sql]}]
                             (str name
                                  (if prepared-factors
                                    " AS NOT MATERIALIZED ("
                                    " AS MATERIALIZED (")
                                  sql ")"))
                           initial-factors)
           step      0]
      (if (empty? variables)
        (let [weight-product
              (str/join " * " (map #(str (:name %) ".w") factors))]
          (str "WITH " (str/join ", " ctes)
               " SELECT COALESCE(" weight-product ", 0)::bigint FROM "
               (str/join ", " (map :name factors))))
        (let [choice
              (first
                (sort-by
                  (fn [var]
                    (let [relevant (filter #(contains? (:vars %) var)
                                           factors)
                          joined   (reduce set/union #{}
                                           (map :vars relevant))]
                      [(count (disj joined var))
                       (count relevant)
                       (reduce + (map (comp count :vars) relevant))
                       (get var-order var)]))
                  variables))
              relevant (filterv #(contains? (:vars %) choice) factors)
              retained (filterv #(not (contains? (:vars %) choice)) factors)
              joined    (reduce set/union #{} (map :vars relevant))
              result-vars (disj joined choice)
              sorted-result-vars (sort-by var-order result-vars)
              source-for
              (fn [var]
                (:name (first (filter #(contains? (:vars %) var)
                                      relevant))))
              result-expressions
              (mapv (fn [var]
                      (str (source-for var) "." (var-name var)))
                    sorted-result-vars)
              result-columns
              (mapv (fn [var expression]
                      (str expression " AS " (var-name var)))
                    sorted-result-vars result-expressions)
              equalities
              (mapcat
                (fn [var]
                  (let [owners (filterv #(contains? (:vars %) var) relevant)
                        left   (:name (first owners))
                        column (var-name var)]
                    (for [right (next owners)]
                      (str left "." column " = " (:name right) "." column))))
                (sort-by var-order joined))
              product (str/join " * "
                                (map #(str (:name %) ".w") relevant))
              name    (str "e" step)
              sql     (str "SELECT "
                           (when (seq result-columns)
                             (str (str/join ", " result-columns) ", "))
                           (when-not (seq result-columns) "COALESCE(")
                           "SUM(" product ")"
                           (when-not (seq result-columns) ", 0)")
                           "::numeric AS w FROM "
                           (str/join ", " (map :name relevant))
                           (when (seq equalities)
                             (str " WHERE " (str/join " AND " equalities)))
                           (when (seq result-expressions)
                             (str " GROUP BY "
                                  (str/join ", " result-expressions))))]
          (recur (conj retained {:name name :vars result-vars})
                 (disj variables choice)
                 (conj ctes (str name " AS MATERIALIZED (" sql ")"))
                 (inc step)))))))

(defn- prepared-factor-spec
  "Describe per-connection temporary factors for one JOB query. A factor is
  filtered and grouped once by every join variable it can expose; subset counts
  subsequently aggregate that compact table instead of rescanning JOB bases."
  [{:keys [from-items conditions]} required-columns excluded-ids join-columns]
  (let [aliases (into #{} (map :alias) from-items)
        occurrences
        (into {}
              (map (fn [[var columns]]
                     [var (->> columns
                               (filter #(contains? aliases (first %)))
                               distinct
                               (sort-by pr-str)
                               vec)]))
              join-columns)
        shared-vars
        (->> occurrences
             (filter (fn [[_ columns]]
                       (< 1 (count (distinct (map first columns))))))
             (map key)
             (sort-by pr-str)
             vec)
        var-name (into {} (map-indexed (fn [i var] [var (str "v" i)]))
                       shared-vars)
        local-equalities
        (reduce-kv
          (fn [result _var columns]
            (reduce
              (fn [result [_alias same-alias]]
                (if (< 1 (count same-alias))
                  (let [[left-alias left-column] (first same-alias)]
                    (into result
                          (for [[right-alias right-column] (next same-alias)]
                            (str left-alias "." left-column " = "
                                 right-alias "." right-column))))
                  result))
              result (group-by first columns)))
          [] occurrences)
        factors
        (into {}
              (map
                (fn [{:keys [alias sql]}]
                  (let [columns
                        (mapv
                          (fn [var]
                            [var (second
                                   (first
                                     (filter #(= alias (first %))
                                             (occurrences var))))])
                          (filter
                            (fn [var]
                              (some #(= alias (first %))
                                    (occurrences var)))
                            shared-vars))
                        ordered-vars (mapv first columns)
                        vars       (set ordered-vars)
                        names      (mapv var-name ordered-vars)
                        selections (map (fn [[var column]]
                                          (str alias "." column " AS "
                                               (var-name var)))
                                        columns)
                        groups     (map (fn [[_var column]]
                                          (str alias "." column))
                                        columns)
                        local-conditions
                        (concat
                          (map :sql
                               (filter #(or (empty? (:aliases %))
                                            (= #{alias} (:aliases %)))
                                       conditions))
                          (for [column (sort (get required-columns alias))]
                            (str alias "." column " IS NOT NULL"))
                          (when-let [ids (seq (get excluded-ids alias))]
                            [(str alias ".id NOT IN ("
                                  (str/join "," ids) ")")])
                          (filter #(str/starts-with? % (str alias "."))
                                  local-equalities))
                        table (str "cidr_oracle_factor_" alias)
                        select-sql
                        (str "SELECT "
                             (when (seq selections)
                               (str (str/join ", " selections) ", "))
                             "COUNT(*)::numeric AS w FROM " sql
                             (when (seq local-conditions)
                               (str " WHERE "
                                    (str/join " AND " local-conditions)))
                             (when (seq groups)
                               (str " GROUP BY " (str/join ", " groups))))
                        projections
                        (mapv
                          (fn [mask]
                            (let [projection-vars
                                  (into #{}
                                        (keep-indexed
                                          (fn [i var]
                                            (when (bit-test mask i) var)))
                                        ordered-vars)
                                  projection-columns
                                  (mapv var-name
                                        (filter projection-vars ordered-vars))
                                  projection-table
                                  (if (= projection-vars vars)
                                    table
                                    (str table "_p" mask))
                                  projection-sql
                                  (str "SELECT "
                                       (when (seq projection-columns)
                                         (str (str/join ", "
                                                        projection-columns)
                                              ", "))
                                       "COALESCE(SUM(w), 0)::numeric AS w FROM "
                                       table
                                       (when (seq projection-columns)
                                         (str " GROUP BY "
                                              (str/join ", "
                                                        projection-columns))))]
                              {:vars projection-vars
                               :table projection-table
                               :statements
                               (when-not (= projection-vars vars)
                                 (vec
                                   (concat
                                     [(str "DROP TABLE IF EXISTS "
                                           projection-table)
                                      (str "CREATE TEMP TABLE "
                                           projection-table
                                           " ON COMMIT PRESERVE ROWS AS "
                                           projection-sql)]
                                     (when (seq projection-columns)
                                       [(str "CREATE INDEX ON "
                                             projection-table " ("
                                             (str/join ", " projection-columns)
                                             ")")])
                                     [(str "ANALYZE "
                                           projection-table)])))}))
                          (range (bit-shift-left 1 (count ordered-vars))))
                        projection-map
                        (into {} (map (juxt :vars :table)) projections)]
                    [alias {:table table
                            :vars vars
                            :columns (vec names)
                            :projections projection-map
                            :statements
                            (vec
                              (concat
                                [(str "DROP TABLE IF EXISTS " table)
                                 (str "CREATE TEMP TABLE " table
                                      " ON COMMIT PRESERVE ROWS AS " select-sql)]
                                (when (seq names)
                                  [(str "CREATE INDEX ON " table " ("
                                        (str/join ", " names) ")")])
                                [(str "ANALYZE " table)]
                                (mapcat :statements projections)))}]))
                from-items))]
    {:var-name var-name
     :factors  factors
     :statements (vec (mapcat :statements (vals factors)))}))

(defn- sql-column
  [schema attr]
  (let [column (str/replace (name attr) "-" "_")]
    (if (= :db.type/ref (get-in schema [attr :db/valueType]))
      (str column "_id")
      column)))

(defn- required-columns
  [database analysis]
  (let [schema (db/-schema database)]
    (reduce
      (fn [result [entity attr _value]]
        (update result (entity-alias entity) (fnil conj #{})
                (sql-column schema attr)))
      {}
      (:patterns analysis))))

(defn- join-columns
  [database analysis]
  (let [schema (db/-schema database)
        entity-columns
        (reduce
          (fn [result entity]
            (update result entity (fnil conj [])
                    [(entity-alias entity) "id"]))
          {} (:entities analysis))]
    (reduce
      (fn [result [entity attr value]]
        (if (and (symbol? value) (qu/free-var? value))
          (update result value (fnil conj [])
                  [(entity-alias entity) (sql-column schema attr)])
          result))
      entity-columns
      (:patterns analysis))))

(declare worker-connection)

(defn- singleton-id-form
  [analysis entity]
  (let [count-form (oracle/subset-count-form analysis #{entity})
        where-idx  (.indexOf count-form :where)
        root-var   (second (nth count-form 1))]
    (vec (concat [:find [root-var '...] :where]
                 (subvec count-form (inc where-idx))))))

(defn- bitset-values
  [^BitSet bits]
  (loop [i (.nextSetBit bits 0)
         result (transient [])]
    (if (neg? i)
      (persistent! result)
      (recur (.nextSetBit bits (inc i)) (conj! result i)))))

(defn- sql-id-query
  [sql-spec entity required joins]
  (str/replace-first
    (subset-sql sql-spec #{entity} required {} joins)
    "SELECT COUNT(*)"
    (str "SELECT " (entity-alias entity) ".id")))

(defn- calibrate-entity!
  [connections url user password database sql-spec analysis required joins
   entity timeout-ms]
  (let [alias      (entity-alias entity)
        table      (:table (first (filter #(= alias (:alias %))
                                          (:from-items sql-spec))))
        base       (or (table-bases table)
                       (throw (ex-info "Unknown JOB entity base"
                                       {:entity entity :table table})))
        query      (cond-> (dp/query->map (singleton-id-form analysis entity))
                     timeout-ms (assoc :timeout timeout-ms))
        ids        (d/q query database)
        ^BitSet datalevin-ids (BitSet.)]
    (doseq [eid ids]
      (let [id (- (long eid) (long base))]
        (when (or (neg? id) (> id Integer/MAX_VALUE))
          (throw (ex-info "Invalid translated JOB entity id"
                          {:entity entity :eid eid :base base})))
        (.set datalevin-ids (int id))))
    (let [^BitSet postgres-ids (BitSet.)
          ^Connection conn (worker-connection connections url user password)]
      (with-open [^Statement statement (.createStatement conn)]
        (when timeout-ms
          (.setQueryTimeout statement
                            (int (max 1 (Math/ceil (/ (double timeout-ms)
                                                      1000.0))))))
        (with-open [result (.executeQuery statement
                                          (sql-id-query sql-spec entity required
                                                        joins))]
          (while (.next result)
            (.set postgres-ids (.getInt result 1)))))
      (let [^BitSet postgres-only (.clone postgres-ids)
            ^BitSet datalevin-only (.clone datalevin-ids)]
        (.andNot postgres-only datalevin-ids)
        (.andNot datalevin-only postgres-ids)
        (when-not (.isEmpty datalevin-only)
          (throw
            (ex-info "Datalevin singleton rows are absent from PostgreSQL"
                     {:entity entity
                      :ids (bitset-values datalevin-only)})))
        (let [excluded (bitset-values postgres-only)]
          (println alias "calibration:" (count ids) "rows,"
                   (count excluded) "PostgreSQL-only ids")
          excluded)))))

(defn- calibrated-exclusions!
  [cache connections url user password database sql-spec analysis required
   joins timeout-ms]
  (into {}
        (for [entity (sort-by pr-str (:entities analysis))
              :let [key [(singleton-id-form analysis entity)
                         (sql-id-query sql-spec entity required joins)]
                    excluded
                    (or (get @cache key)
                        (let [value
                              (calibrate-entity!
                                connections url user password database sql-spec
                                analysis required joins entity timeout-ms)]
                          (get (swap! cache
                                      #(if (contains? % key) %
                                           (assoc % key value)))
                               key)))]]
          [(entity-alias entity) excluded])))

(defn- worker-connection
  [connections url user password]
  (let [thread (Thread/currentThread)]
    (or (get @connections thread)
        (let [^Connection conn (DriverManager/getConnection url user password)]
          (get (swap! connections
                      #(if (contains? % thread) % (assoc % thread conn)))
               thread)))))

(defn- prepare-factors!
  [^Connection connection statements timeout-ms]
  (with-open [^Statement statement (.createStatement connection)]
    (when timeout-ms
      (.setQueryTimeout statement
                        (int (max 1 (Math/ceil (/ (double timeout-ms)
                                                  1000.0))))))
    (doseq [sql statements]
      (.execute statement sql))))

(defn- sql-count
  [connections url user password sql timeout-ms]
  (let [^Connection conn (worker-connection connections url user password)]
    (with-open [^Statement statement (.createStatement conn)]
      (when timeout-ms
        (.setQueryTimeout statement
                          (int (max 1 (Math/ceil (/ (double timeout-ms)
                                                    1000.0))))))
      (with-open [result (.executeQuery statement sql)]
        (when-not (.next result)
          (throw (ex-info "COUNT(*) returned no row" {:sql sql})))
        (.getLong result 1)))))

(defn- submit-validation
  [^ExecutorService executor count-subset db analysis entities timeout-ms]
  (.submit executor
           ^Callable
           (bound-fn []
             (count-subset db analysis entities timeout-ms nil))))

(defn- validate-existing!
  [^ExecutorService executor count-subset db analysis existing timeout-ms
   query-name]
  (let [jobs
        (mapv (fn [[entities expected]]
                {:entities entities
                 :expected expected
                 :future (submit-validation executor count-subset db analysis
                                            entities timeout-ms)})
              (sort-by (juxt (comp count key) (comp pr-str key)) existing))]
    (doseq [[i {:keys [entities expected future]}] (map-indexed vector jobs)]
      (let [actual (.get ^Future future)]
        (when-not (= (long expected) (long actual))
          (throw (ex-info "SQL and Datalevin exact cardinalities disagree"
                          {:query query-name
                           :entities entities
                           :datalevin expected
                           :sql actual})))
        (when (or (= i (dec (count jobs))) (zero? (mod (inc i) 100)))
          (println query-name "validated" (inc i) "/" (count jobs)))))))

(defn run
  [{:keys [db-path sql-dir output-dir queries timeout-ms parallelism
           url user password validate-existing? skip-complete?]
    :or   {db-path "db"
           sql-dir "queries"
           output-dir "results/cidr-exact-cardinalities"
           timeout-ms 600000
           parallelism 2
           url "jdbc:postgresql://localhost:5432/postgres"
           user (or (System/getProperty "pg.user")
                    (System/getenv "USER"))
           password ""
           validate-existing? true
           skip-complete? false}}]
  (when-not (and (integer? parallelism) (pos? parallelism))
    (throw (ex-info "SQL oracle parallelism must be a positive integer"
                    {:parallelism parallelism})))
  (let [conn        (d/get-conn db-path)
        database    (d/db conn)
        executor    (Executors/newFixedThreadPool (int parallelism))
        connections (atom {})
        calibration-cache (atom {})]
    (try
      (let [shared-file   (io/file output-dir "shared.edn")
            shared-counts (atom (oracle/read-shared-checkpoint shared-file))
            query-symbols
            (cond->> (oracle/selected-query-symbols queries)
              skip-complete?
              (filterv
                (fn [query-sym]
                  (let [query-name (oracle/query-name query-sym)
                        output-file (io/file output-dir
                                             (str query-name ".edn"))
                        existing (set (keys (oracle/read-checkpoint
                                              output-file)))
                        required (set (oracle/all-connected-subsets
                                        (oracle/query-graph
                                          database
                                          (oracle/query-value query-sym))))
                        complete? (= required existing)]
                    (when complete?
                      (println query-name "already complete; skipped"))
                    (not complete?)))))]
        (io/make-parents shared-file)
        (with-open [shared-writer (io/writer shared-file :append true)]
          (doseq [query-sym query-symbols]
            (let [query-name (oracle/query-name query-sym)
                  sql-file   (io/file sql-dir (str query-name ".sql"))
                  _          (when-not (.exists sql-file)
                               (throw (ex-info "JOB SQL file is missing"
                                               {:query query-name
                                                :file (str sql-file)})))
                  sql-spec   (parse-job-sql (slurp sql-file))
                  analysis   (oracle/query-analysis
                               (oracle/query-value query-sym))
                  required   (required-columns database analysis)
                  joins      (join-columns database analysis)
                  calibration-file
                  (io/file output-dir (str query-name ".calibration.edn"))
                  exclusions
                  (if (.exists calibration-file)
                    (edn/read-string (slurp calibration-file))
                    (let [value
                          (calibrated-exclusions!
                            calibration-cache connections url user password
                            database sql-spec analysis required joins
                            timeout-ms)]
                      (io/make-parents calibration-file)
                      (spit calibration-file (str (pr-str value) "\n"))
                      value))
                  factor-spec (prepared-factor-spec sql-spec required
                                                    exclusions joins)
                  prepared-threads (atom #{})
                  sql-aliases (into #{} (map :alias) (:from-items sql-spec))
                  datalog-aliases (into #{} (map entity-alias)
                                        (:entities analysis))
                  _          (when-not (= sql-aliases datalog-aliases)
                               (throw
                                 (ex-info
                                   "JOB SQL and Datalog relation sets differ"
                                   {:query query-name
                                    :sql-aliases sql-aliases
                                    :datalog-aliases datalog-aliases})))
                  output-file (str (io/file output-dir
                                            (str query-name ".edn")))
                  existing    (oracle/read-checkpoint output-file)
                  count-subset
                  (fn [_db _analysis entities per-query-timeout-ms _known]
                    (let [thread     (Thread/currentThread)
                          connection (worker-connection connections url user
                                                        password)]
                      (when-not (contains? @prepared-threads thread)
                        (prepare-factors! connection (:statements factor-spec)
                                          per-query-timeout-ms)
                        (swap! prepared-threads conj thread))
                      (sql-count connections url user password
                                 (factorized-subset-sql
                                   (assoc sql-spec
                                          :prepared-factors factor-spec)
                                   entities required exclusions joins)
                                 per-query-timeout-ms)))]
              (when (and validate-existing? (seq existing))
                (validate-existing! executor count-subset database analysis
                                    existing timeout-ms query-name))
              (oracle/precompute-query!
                database query-sym
                {:output-file output-file
                 :timeout-ms timeout-ms
                 :shared-counts shared-counts
                 :shared-writer shared-writer
                 :executor executor
                 :count-subset count-subset
                 :count-method :postgres-exact})
              ;; Closing the per-thread connections drops all temporary factor
              ;; projections before the next JOB query is prepared.
              (doseq [[_ ^Connection connection] @connections]
                (.close connection))
              (reset! connections {})))))
      (finally
        (.shutdownNow ^ExecutorService executor)
        (doseq [[_ ^Connection connection] @connections]
          (.close connection))
        (d/close conn)))))
