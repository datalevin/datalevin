(ns ldbc-snb-bench.neo4j
  "Embedded Neo4j runner for the shared LDBC SNB read-query latency harness."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [ldbc-snb-bench.core :as core]
   [ldbc-snb-bench.harness :as harness]
   [ldbc-snb-bench.queries.interactive :as ic]
   [ldbc-snb-bench.queries.short :as is])
  (:import
   [java.nio.file Path]
   [java.time Instant LocalDate LocalDateTime OffsetDateTime ZoneOffset
    ZonedDateTime]
   [java.util Date List Map]
   [org.neo4j.configuration GraphDatabaseSettings
    GraphDatabaseSettings$CypherVersion]
   [org.neo4j.configuration.connectors BoltConnector HttpConnector
    HttpsConnector]
   [org.neo4j.dbms.api DatabaseManagementService
    DatabaseManagementServiceBuilder]
   [org.neo4j.graphdb GraphDatabaseService Label RelationshipType Result
    Transaction]
   [org.neo4j.graphdb.schema IndexDefinition]
   [org.neo4j.io ByteUnit]
   [org.neo4j.logging NullLogProvider]))

(def neo4j-version
  (or (some-> DatabaseManagementServiceBuilder
              .getPackage
              .getImplementationVersion)
      "unknown"))
(def default-database "neo4j")
(def default-cypher-path "neo4j/queries.cypher")
(def default-schema-path "neo4j/schema.cypher")
(def default-data-path "neo4j/data")
(def default-home-path "neo4j/runtime/embedded-home")
(def default-page-cache "4g")
(def default-results-path "neo4j/results/results.csv")
(def default-perf-path "neo4j/results/perf.csv")
(def default-report-path "neo4j/results/report.edn")

(def ^:private query-marker
  #"^//\s+((?:IC|IS)\d+)(?:\s.*)?$")

(defn- finish-query
  [queries query-name lines]
  (if query-name
    (let [query (-> (str/join "\n" lines)
                    str/trim
                    (str/replace #";\s*$" ""))]
      (when (str/blank? query)
        (throw (ex-info (str "Empty Cypher query block: " query-name)
                        {:query query-name})))
      (assoc queries query-name query))
    queries))

(defn load-cypher-queries
  "Load // ICn and // ISn blocks from the shared Cypher source file."
  [path]
  (let [{:keys [queries query-name lines]}
        (reduce
          (fn [{:keys [queries query-name lines]} line]
            (if-let [[_ next-name] (re-matches query-marker line)]
              {:queries (finish-query queries query-name lines)
               :query-name next-name
               :lines []}
              {:queries queries
               :query-name query-name
               :lines (if query-name (conj lines line) lines)}))
          {:queries {} :query-name nil :lines []}
          (str/split-lines (slurp path)))]
    (finish-query queries query-name lines)))

(defn- kebab->camel
  [value]
  (str/replace value #"-([a-z])"
               (fn [[_ letter]] (str/upper-case letter))))

(defn- parameter-value
  [value]
  (cond
    (instance? Date value) (str (.toInstant ^Date value))
    (instance? Instant value) (str value)
    (keyword? value) (name value)
    (set? value) (mapv parameter-value (sort-by str value))
    (sequential? value) (mapv parameter-value value)
    :else value))

(defn neo4j-parameters
  "Convert harness keyword parameters to Neo4j parameter names/types."
  [params]
  (into {}
        (map (fn [[key value]]
               [(kebab->camel (name key)) (parameter-value value)]))
        params))

(declare result-value)

(defn- temporal-date
  [value]
  (cond
    (instance? ZonedDateTime value)
    (Date/from (.toInstant ^ZonedDateTime value))

    (instance? OffsetDateTime value)
    (Date/from (.toInstant ^OffsetDateTime value))

    (instance? LocalDateTime value)
    (Date/from (.toInstant ^LocalDateTime value ZoneOffset/UTC))

    (instance? LocalDate value)
    (Date/from (.toInstant (.atStartOfDay ^LocalDate value) ZoneOffset/UTC))))

(defn result-value
  "Convert Neo4j values to deterministic Clojure/EDN values."
  [value]
  (cond
    (nil? value) nil
    (or (instance? ZonedDateTime value)
        (instance? OffsetDateTime value)
        (instance? LocalDateTime value)
        (instance? LocalDate value))
    (temporal-date value)

    (instance? Map value)
    (into (sorted-map)
          (map (fn [[key item]] [(str key) (result-value item)]))
          value)

    (instance? List value) (mapv result-value value)
    (and value (.isArray (class value))) (mapv result-value (seq value))
    :else value))

(defn- record->row
  [query-name columns ^Map record]
  (let [values (mapv #(result-value (.get record %)) columns)
        label (first values)]
    (when-not (= query-name label)
      (throw (ex-info "Cypher query returned the wrong query label"
                      {:expected query-name :actual label})))
    (subvec values 1)))

(defn run-query
  [^GraphDatabaseService db cypher-queries query-def params]
  (let [query-name (:name query-def)
        query (get cypher-queries query-name)]
    (when-not query
      (throw (ex-info (str "Missing Cypher query: " query-name)
                      {:query query-name})))
    (let [query (if (= :measurement harness/*benchmark-phase*)
                  (str "CYPHER cache=skip\n" query)
                  query)
          parameters (neo4j-parameters params)
          start (System/nanoTime)
          output
          (with-open [^Transaction tx (.beginTx db)
                      ^Result result (.execute tx query ^Map parameters)]
            (let [all-columns (vec (.columns result))
                  rows (loop [rows []]
                         (if (.hasNext result)
                           (recur (conj rows
                                        (record->row query-name all-columns
                                                     (.next result))))
                           rows))]
              (.commit tx)
              {:columns (vec (rest all-columns))
               :rows rows}))
          elapsed (/ (- (System/nanoTime) start) 1000000.0)]
      {:name query-name
       :planning-time 0.0
       :execution-time elapsed
       :result-count (count (:rows output))
       :rows (:rows output)
       :columns (:columns output)})))

(defn- label-names
  [values]
  (mapv #(.name ^Label %) values))

(defn- relationship-type-names
  [values]
  (mapv #(.name ^RelationshipType %) values))

(defn- index-info
  [^IndexDefinition index]
  (let [node-index? (.isNodeIndex index)
        relationship-index? (.isRelationshipIndex index)]
    {:name (.getName index)
     :type (str (.getIndexType index))
     :entity-type (if node-index? "NODE" "RELATIONSHIP")
     :labels-or-types
     (cond
       (and node-index? (not (.isMultiTokenIndex index)))
       (not-empty (label-names (.getLabels index)))

       (and relationship-index? (not (.isMultiTokenIndex index)))
       (not-empty
         (relationship-type-names (.getRelationshipTypes index)))

       :else nil)
     :properties (let [properties (vec (.getPropertyKeys index))]
                   (when (seq properties) properties))
     :owning-constraint (when (.isConstraintIndex index)
                          (.getName index))}))

(defn- database-info
  [^GraphDatabaseService db page-cache-bytes]
  (with-open [^Transaction tx (.beginTx db)]
    (let [indexes (->> (.getIndexes (.schema tx))
                       (map index-info)
                       (sort-by :name)
                       vec)]
      (.commit tx)
      {:name "Neo4j Kernel"
       :version neo4j-version
       :edition "community"
       :deployment :embedded
       :settings
       (sorted-map
         "db.query.default_language" "CYPHER_25"
         "server.memory.pagecache.size"
         (ByteUnit/bytesToString page-cache-bytes))
       :indexes indexes})))

(defn- canonical-path
  ^Path [path]
  (.toPath (.getCanonicalFile (io/file path))))

(defn- open-database
  [{:keys [database home-path page-cache-bytes]} db-path]
  (let [home-file (io/file home-path)]
    (when-not (or (.isDirectory home-file) (.mkdirs home-file))
      (throw (ex-info (str "Could not create Neo4j embedded home: " home-path)
                      {:home-path home-path})))
    (let [^Path home (canonical-path home-path)
          ^Path data (canonical-path db-path)
          builder
          (doto (DatabaseManagementServiceBuilder. home)
            (.setUserLogProvider (NullLogProvider/getInstance))
            (.setConfig GraphDatabaseSettings/data_directory data)
            (.setConfig GraphDatabaseSettings/pagecache_memory
                        (long page-cache-bytes))
            (.setConfig GraphDatabaseSettings/default_language
                        GraphDatabaseSettings$CypherVersion/Cypher25)
            (.setConfig GraphDatabaseSettings/initial_default_database
                        database)
            (.setConfig GraphDatabaseSettings/auth_enabled false)
            (.setConfig BoltConnector/enabled false)
            (.setConfig HttpConnector/enabled false)
            (.setConfig HttpsConnector/enabled false))
          management (try
                       (.build builder)
                       (catch Throwable cause
                         (throw
                           (ex-info
                             (str "Could not open the embedded Neo4j database. "
                                  "Ensure no Neo4j server is using " db-path)
                             {:db-path db-path :home-path home-path}
                             cause))))]
      (try
        (let [db (.database ^DatabaseManagementService management database)]
          {:management management
           :database-service db
           :server (database-info db page-cache-bytes)
           :home-path (str home)
           :data-path (str data)})
        (catch Throwable cause
          (.shutdown ^DatabaseManagementService management)
          (throw cause))))))

(defn- close-database
  [{:keys [^DatabaseManagementService management]}]
  (.shutdown management))

(defn- connection-options
  []
  (let [page-cache (or (System/getenv "NEO4J_PAGECACHE")
                       default-page-cache)]
    {:database (or (System/getenv "NEO4J_DATABASE") default-database)
     :home-path (or (System/getenv "NEO4J_EMBEDDED_HOME") default-home-path)
     :page-cache page-cache
     :page-cache-bytes (ByteUnit/parse page-cache)
     :cypher-path (or (System/getenv "NEO4J_CYPHER") default-cypher-path)}))

(def ^:private connection-value-options
  #{"--database" "--home" "--page-cache" "--cypher"})

(defn- assoc-connection-option
  [connection option value]
  (case option
    "--database" (assoc connection :database value)
    "--home" (assoc connection :home-path value)
    "--page-cache" (assoc connection
                           :page-cache value
                           :page-cache-bytes (ByteUnit/parse value))
    "--cypher" (assoc connection :cypher-path value)))

(defn parse-args
  [args]
  (loop [remaining args
         connection (connection-options)
         harness-args ["--db" default-data-path
                       "--results" default-results-path
                       "--perf" default-perf-path
                       "--output" default-report-path]]
    (if-let [arg (first remaining)]
      (if (contains? connection-value-options arg)
        (let [value (second remaining)]
          (when-not value
            (throw (ex-info (str "Missing value for " arg) {:option arg})))
          (recur (nnext remaining)
                 (assoc-connection-option connection arg value)
                 harness-args))
        (recur (next remaining) connection (conj harness-args arg)))
      {:connection connection
       :benchmark (harness/parse-bench-args harness-args)})))

(defn usage
  []
  (str
    "LDBC SNB read-query benchmark for Neo4j Community Embedded\n\n"
    "Usage:\n"
    "  clj -M:neo4j-bench [NEO4J OPTIONS] [HARNESS OPTIONS] [IC1 IS1 ...]\n\n"
    "Embedded Neo4j options:\n"
    "  --database NAME        Database (default neo4j)\n"
    "  --home PATH            Embedded home (default neo4j/runtime/embedded-home)\n"
    "  --page-cache SIZE      Page cache (default 4g)\n"
    "  --cypher PATH          Query file (default neo4j/queries.cypher)\n\n"
    (harness/usage)))

(defn run-benchmark
  [connection-options benchmark-options]
  (when (:query-cache? benchmark-options)
    (throw (ex-info "Neo4j has no equivalent application result-cache mode"
                    {:option "--query-cache"})))
  (let [cypher-path (:cypher-path connection-options)
        cypher-queries (load-cypher-queries cypher-path)
        expected-names (mapv :name (concat ic/all-queries is/all-queries))]
    (when-not (= (set expected-names) (set (keys cypher-queries)))
      (throw (ex-info "Cypher query file does not define the complete suite"
                      {:expected expected-names
                       :actual (sort (keys cypher-queries))})))
    (harness/run-benchmark!
      {:all-query-defs (vec (concat ic/all-queries is/all-queries))
       :sample-suite core/sample-suite
       :sample-result-counts core/sample-result-counts
       :get-conn (fn [db-path]
                   (open-database connection-options db-path))
       :close-conn close-database
       :connection->db :database-service
       :run-query (fn [db query-def params]
                    (run-query db cypher-queries query-def params))
       :write-results-rows core/write-results-rows
       :system-name "Neo4j Community Embedded"
       :measurement-cache-policy :fresh-parse-and-plan
       :report-extra
       (fn [connection]
         {:neo4j {:server (:server connection)
                  :access-mode :embedded
                  :embedded-artifact-version neo4j-version
                  :measurement-cache-implementation "CYPHER cache=skip"
                  :database (:database connection-options)
                  :embedded-home (:home-path connection)
                  :data-path (:data-path connection)
                  :page-cache-bytes (:page-cache-bytes connection-options)
                  :cypher-file (.getCanonicalPath (io/file cypher-path))
                  :cypher-sha256 (harness/sha256 (slurp cypher-path))
                  :schema-file
                  (.getCanonicalPath (io/file default-schema-path))
                  :schema-sha256
                  (harness/sha256 (slurp default-schema-path))}})}
      benchmark-options)))

(defn -main
  [& args]
  (try
    (let [{:keys [connection benchmark]} (parse-args args)]
      (if (:help? benchmark)
        (println (usage))
        (let [report (run-benchmark connection benchmark)]
          (when (pos? (:exit-code report))
            (throw (ex-info "One or more Neo4j benchmark queries failed"
                            {:exit-code (:exit-code report)}))))))
    (finally
      (shutdown-agents))))
