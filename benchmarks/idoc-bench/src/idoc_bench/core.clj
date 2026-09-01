(ns idoc-bench.core
  "YCSB-style benchmark with idoc query workload."
  (:require
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.idoc :as idoc]
   [datalevin.util :as u]
   [jsonista.core :as json]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [next.jdbc.sql :as sql])
  (:import
   [com.mongodb WriteConcern]
   [com.mongodb.client MongoClients]
   [com.mongodb.client.model Filters Projections]
   [datalevin.cpp Util]
   [java.lang.management ManagementFactory]
   [java.nio.charset StandardCharsets]
   [java.security MessageDigest]
   [java.time Instant]
   [java.util ArrayList Random UUID]
   [java.util.concurrent Callable ExecutionException Executors TimeUnit]
   [org.bson Document]
   [org.postgresql.util PGobject]))

(def schema
  {:mem/doc {:db/valueType :db.type/idoc
             :db/domain    "memory"}})

(def workloads
  {:A {:read 50 :update 50}
   :C {:read 100}
   :F {:rmw 100}})

(def durability-profiles #{:strict :relaxed})
(def run-roles #{:warmup :measurement})

(defonce ^:private jvm-instance-id
  (str (UUID/randomUUID)))

(def tags
  ["urgent" "todo" "followup" "meeting" "project" "customer" "issue"
   "feature" "blocker" "idea" "note" "action" "bug" "enhancement" "question"
   "help" "wontfix" "duplicate" "invalid" "documentation" "security" "test"
   "refactor" "performance" "ux" "backend" "frontend" "mobile" "api" "database"])

(def langs
  ["en" "es" "fr" "de" "zh" "ja" "ko" "pt" "ru" "it" "nl" "pl" "sv" "da"
   "no" "fi" "cs" "hu" "ro" "tr" "ar" "he" "th" "vi" "id" "ms" "hi" "bn"])

(def personas
  ["developer" "manager" "founder" "analyst" "support" "designer" "architect"
   "qa" "devops" "sre" "pm" "cto" "ceo" "intern" "contractor" "consultant"])

(def topics
  ["roadmap" "billing" "onboarding" "incident" "design" "infra" "security"
   "performance" "migration" "integration" "deployment" "testing" "review"
   "planning" "retrospective" "standup" "demo" "release" "hotfix" "rollback"])

(def sources
  ["chat" "email" "call" "doc" "note" "slack" "teams" "zoom" "jira" "github"
   "confluence" "notion" "linear" "asana" "trello" "discord" "webhook" "api"])

(def cities
  ["SF" "NYC" "LA" "SEA" "LON" "BER" "PAR" "TOK" "SYD" "SIN" "HKG" "DUB"
   "AMS" "TOR" "VAN" "CHI" "BOS" "ATL" "MIA" "DEN" "PHX" "DAL" "HOU" "PDX"
   "AUS" "MSP" "DET" "PHL" "SLC" "STL" "ORL" "TPA" "CLT" "RDU" "IND" "CLE"])

(def teams
  ["red" "blue" "green" "yellow" "purple" "orange" "pink" "cyan" "alpha"
   "beta" "gamma" "delta" "epsilon" "zeta" "eta" "theta" "iota" "kappa"])

(def entities
  ["acme" "globex" "initech" "umbrella" "stark" "wayne" "oscorp" "lexcorp"
   "cyberdyne" "weyland" "tyrell" "massive" "abstergo" "aperture" "black-mesa"
   "vault-tec" "capsule" "waystar" "hooli" "pied-piper" "raviga" "endframe"])

(def kinds
  ["chat" "email" "note" "task" "ticket" "alert" "notification" "reminder"
   "event" "meeting" "call" "message" "comment" "review" "approval" "request"])

(def base-ts 1700000000000)

(def q-idoc
  '[:find ?e
    :in $ ?q
    :where [(idoc-match $ :mem/doc ?q nil) [[?e _ _]]]])

(def default-opts
  {:system      :datalevin
   :workload    :C
   :records     10000
   :ops         10000
   :warmup      1000
   :threads     1
   :batch-size  1000
   :idoc-ratio  30
   :idoc-trace? false
   :verify?     true
   :hotset      1.0
   :seed        42
   :durability-profile :strict
   :run-role    :measurement
   :output      nil
   :dir         nil
   :dtlv-uri    nil
   :keep-db?    false
   :env-flags   nil
   :pg-url      "jdbc:postgresql://localhost:5432/postgres"
   :pg-user     nil
   :pg-password nil
   :pg-table    "idoc_bench_docs"
   :sqlite-path nil
   :mongo-uri   "mongodb://localhost:27017"
   :mongo-db    "idoc_bench"
   :mongo-coll  "docs"})

(def ^:private json-mapper
  (json/object-mapper {:encode-key-fn name
                       :decode-key-fn keyword}))

(defn- encode-json
  [v]
  (json/write-value-as-string v json-mapper))

(defn- decode-json
  [^String s]
  (json/read-value s json-mapper))

(defn- rand-choice
  [^Random r coll]
  (nth coll (.nextInt r (count coll))))

(defn- rand-tags
  [^Random r max-count]
  (let [n (inc (.nextInt r max-count))]
    (vec (repeatedly n #(rand-choice r tags)))))

(defn- rand-entities
  [^Random r]
  (let [n (inc (.nextInt r 3))]
    (vec (repeatedly n #(rand-choice r entities)))))

(defn- make-event
  [^Random r id idx]
  {:ts     (+ base-ts id (* idx 1000) (.nextInt r 100000))
   :kind   (rand-choice r kinds)
   :tags   (rand-tags r 3)
   :entity {:name (rand-choice r entities)}
   :score  (double (.nextDouble r))})

(defn- make-doc
  [^Random r id]
  {:profile {:age     (+ 18 (.nextInt r 50))
             :lang    (rand-choice r langs)
             :persona (rand-choice r personas)}
   :stats   {:score     (double (.nextDouble r))
             :last_seen (+ base-ts (.nextInt r 2000000))}
   :facts   {"city" (rand-choice r cities)
             "team" (rand-choice r teams)}
   :memory  {:topic    (rand-choice r topics)
             :source   (rand-choice r sources)
             :entities (rand-entities r)}
   :tags    (rand-tags r 4)
   :events  (vec (map-indexed (fn [idx _] (make-event r id idx))
                              (range (inc (.nextInt r 4)))))})

(defn- generate-docs
  [records seed]
  (let [r (Random. seed)]
    (vec (map (fn [id] (make-doc r id)) (range 1 (inc records))))))

(defn- update-doc
  [doc {:keys [score last-seen]}]
  (-> doc
      (assoc-in [:stats :score] score)
      (assoc-in [:stats :last_seen] last-seen)))

(defn- update-doc-patch
  [doc {:keys [score last-seen] :as operation}]
  [(update-doc doc operation)
   [[:set [:stats :score] score]
    [:set [:stats :last_seen] last-seen]]])

(defn- rmw-doc
  [doc {:keys [last-seen]}]
  (-> doc
      (update-in [:stats :score] (fnil #(+ % 0.01) 0.0))
      (assoc-in [:stats :last_seen] last-seen)))

(defn- rmw-doc-patch
  [doc {:keys [last-seen] :as operation}]
  (let [score     (double ((fnil #(+ % 0.01) 0.0) (get-in doc [:stats :score])))
        doc'      (rmw-doc doc operation)]
    [doc' [[:set [:stats :score] score]
           [:set [:stats :last_seen] last-seen]]]))

(defn- rand-id
  [^Random r records hotset]
  (let [max-id (if (and hotset (< hotset 1.0))
                 (max 1 (int (* records hotset)))
                 records)]
    (inc (.nextInt r max-id))))

(def query-types [:nested :range :wildcard-one :wildcard-depth :array])

(defn- rand-query-spec
  ([^Random r]
   (rand-query-spec r (rand-choice r query-types)))
  ([^Random r query-type]
   (case query-type
    :nested {:type :nested :value (rand-choice r langs)}
    :range (let [lo (double (* 0.1 (.nextInt r 8)))      ;; 0.0 to 0.7
                 hi (double (+ lo 0.1 (* 0.1 (.nextInt r 3))))] ;; lo+0.1 to lo+0.3
             {:type :range :lo lo :hi (min hi 1.0)})
    :wildcard-one {:type :wildcard-one :value (rand-choice r cities)}
    :wildcard-depth {:type :wildcard-depth :value (rand-choice r entities)}
    :array {:type :array :value (rand-choice r tags)})))

(defn- spec->idoc-query
  [spec]
  (case (:type spec)
    :nested {:profile {:lang (:value spec)}}
    :range (list '< (:lo spec) [:stats :score] (:hi spec))
    :wildcard-one {:facts {:? (:value spec)}}
    :wildcard-depth {:* {:entity {:name (:value spec)}}}
    :array {:events {:tags (:value spec)}}))

(defn- build-selector
  [weights]
  (let [table (loop [acc []
                     sum 0
                     [[op w] & more] weights]
                (if op
                  (recur (conj acc [op (+ sum w)])
                         (+ sum w)
                         more)
                  acc))
        total (double (second (peek table)))]
    (fn [^Random r]
      (let [x (* total (.nextDouble r))]
        (loop [[[op bound] & more] table]
          (if (or (nil? op) (<= x bound) (empty? more))
            op
            (recur more)))))))

(defn- workload->weights
  [workload idoc-ratio]
  (let [base (get workloads workload)]
    (cond-> (vec base)
      (pos? idoc-ratio) (conj [:idoc idoc-ratio]))))

(defn- generate-operation
  [selector ^Random r {:keys [records hotset]}]
  (let [op (selector r)]
    (case op
      :read {:op op :id (rand-id r records hotset)}
      :update {:op        op
               :id        (rand-id r records hotset)
               :score     (double (.nextDouble r))
               :last-seen (+ base-ts (.nextInt r 2000000))}
      :rmw {:op        op
            :id        (rand-id r records hotset)
            :last-seen (+ base-ts (.nextInt r 2000000))}
      :idoc {:op op :spec (rand-query-spec r)})))

(defn- generate-schedule
  [opts operation-count seed]
  (let [r        (Random. seed)
        selector (build-selector (workload->weights (:workload opts)
                                                    (:idoc-ratio opts)))]
    (mapv (fn [index]
            (assoc (generate-operation selector r opts)
                   :schedule-index index))
          (range operation-count))))

(defn- schedule-digest
  [schedule]
  (let [digest (.digest (MessageDigest/getInstance "SHA-256")
                        (.getBytes (pr-str schedule) StandardCharsets/UTF_8))]
    (apply str (map #(format "%02x" (bit-and (int %) 0xff)) digest))))

(defn- percentile
  [sorted-values fraction]
  (let [n     (count sorted-values)
        index (min (dec n)
                   (dec (long (Math/ceil (* fraction n)))))]
    (nth sorted-values (max 0 index))))

(defn- latency-summary
  [samples]
  (let [values (vec (sort (map :latency-ns samples)))
        n      (count values)
        in-ms  #(/ (double %) 1e6)]
    (when (pos? n)
      {:count  n
       :min    (in-ms (first values))
       :median (in-ms (percentile values 0.5))
       :p95    (in-ms (percentile values 0.95))
       :p99    (in-ms (percentile values 0.99))
       :max    (in-ms (peek values))
       :mean   (in-ms (/ (reduce + values) (double n)))})))

(defn- summarize-samples
  [samples]
  {:operations
   (into (sorted-map)
         (map (fn [[op op-samples]] [op (latency-summary op-samples)]))
         (group-by :op samples))
   :idoc-shapes
   (into (sorted-map)
         (map (fn [[shape shape-samples]]
                [shape (latency-summary shape-samples)]))
         (group-by :shape (filter :shape samples)))})

(def ^:private empty-idoc-trace
  {:count          0
   :cand-sum       0
   :verify-sum     0
   :doc-fetch-sum  0
   :match-sum      0
   :elapsed-ns-sum 0
   :exact-count    0})

(defn- trace-key
  [spec event]
  (if-let [domain (:domain event)]
    [(:type spec) domain]
    (:type spec)))

(defn- accumulate-idoc-trace
  [m event]
  (let [m (or m empty-idoc-trace)
        cand-count   (long (or (:candidate-count event) 0))
        verify-count (long (or (:verify-count event) 0))
        doc-fetches  (long (or (:doc-fetch-count event) 0))
        match-count  (long (or (:match-count event) 0))
        elapsed-ns   (long (or (:elapsed-ns event) 0))
        exact?       (boolean (:exact? event))]
    (-> m
        (update :count inc)
        (update :cand-sum + cand-count)
        (update :verify-sum + verify-count)
        (update :doc-fetch-sum + doc-fetches)
        (update :match-sum + match-count)
        (update :elapsed-ns-sum + elapsed-ns)
        (update :exact-count + (if exact? 1 0)))))

(defn- update-idoc-trace!
  [trace spec event]
  (when (and trace (= (:event event) :idoc-match-domain))
    (let [k (trace-key spec event)]
      (swap! trace update k accumulate-idoc-trace event))))

(defn- print-idoc-trace
  [trace]
  (when (and trace (seq @trace))
    (println)
    (println "idoc trace")
    (println "type\tcount\tavg-cand\tavg-verify\tavg-docs\tavg-match\tavg-ms\texact%")
    (doseq [[k {:keys [count cand-sum verify-sum doc-fetch-sum
                       match-sum elapsed-ns-sum exact-count]}]
            (sort-by key @trace)]
      (let [avg     (fn [sum] (if (pos? count) (/ sum (double count)) 0.0))
            avg-ms  (if (pos? count) (/ elapsed-ns-sum (* count 1e6)) 0.0)
            exact-p (if (pos? count) (* 100.0 (/ exact-count count)) 0.0)
            kstr    (if (vector? k)
                      (str (name (first k)) "/" (second k))
                      (name k))]
        (println (str kstr
                      "\t" count
                      "\t" (format "%.1f" (avg cand-sum))
                      "\t" (format "%.1f" (avg verify-sum))
                      "\t" (format "%.1f" (avg doc-fetch-sum))
                      "\t" (format "%.1f" (avg match-sum))
                      "\t" (format "%.4f" (double avg-ms))
                      "\t" (format "%.1f" (double exact-p))))))))

(defn- print-latency-table
  [title summaries]
  (when (seq summaries)
    (println)
    (println title)
    (println "name\tcount\tmean-ms\tmedian-ms\tp95-ms\tp99-ms")
    (doseq [[label {:keys [count mean median p95 p99]}] summaries]
      (println (str (name label)
                    "\t" count
                    "\t" (format "%.4f" (double mean))
                    "\t" (format "%.4f" (double median))
                    "\t" (format "%.4f" (double p95))
                    "\t" (format "%.4f" (double p99)))))))

(defn- print-summary
  [summary total-ops elapsed-ms]
  (println "Total ops:" total-ops)
  (println "Elapsed (ms):" (format "%.2f" (double elapsed-ms)))
  (println "Throughput (ops/sec):"
           (format "%.2f"
                   (double (if (pos? elapsed-ms)
                             (/ total-ops (/ elapsed-ms 1000.0))
                             0.0))))
  (print-latency-table "operation latency" (:operations summary))
  (print-latency-table "idoc latency by query shape" (:idoc-shapes summary)))

(defn- load-docs-datalevin!
  [conn docs batch-size]
  (doseq [batch (partition-all batch-size (map-indexed
                                           (fn [idx doc]
                                             {:db/id  (inc idx)
                                              :mem/doc doc})
                                           docs))]
    (d/transact! conn batch)))

(defn- pg-jsonb
  [^String s]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue s)))

(defn- sql-doc->json
  [v]
  (cond
    (nil? v) nil
    (instance? PGobject v) (.getValue ^PGobject v)
    :else (str v)))

(defn- first-row-value
  [row]
  (when (map? row)
    (first (vals row))))

(defn- sql-query
  [conn sql params]
  (jdbc/execute! conn (into [sql] params)
                 {:builder-fn rs/as-unqualified-lower-maps}))

(defn- sql-query-ids
  [conn sql params]
  (into #{} (map :id) (sql-query conn sql params)))

(defn- pg-synchronous-commit
  [durability-profile]
  (case durability-profile
    :relaxed "off"
    "on"))

(defn- pg-conn
  [ds durability-profile]
  (let [conn (jdbc/get-connection ds)
        mode (pg-synchronous-commit durability-profile)]
    (try
      (jdbc/execute! conn [(str "SET synchronous_commit = " mode)])
      (let [actual (str (first-row-value
                          (jdbc/execute-one!
                            conn ["SHOW synchronous_commit"]
                            {:builder-fn rs/as-unqualified-lower-maps})))]
        (when-not (= mode actual)
          (throw (ex-info "PostgreSQL did not enter the requested durability mode"
                          {:expected mode :actual actual}))))
      conn
      (catch Throwable t
        (.close conn)
        (throw t)))))

(defn- pg-storage
  [conn durability-profile]
  (let [metadata (.getMetaData conn)]
    {:engine :postgresql
     :server-version (.getDatabaseProductVersion metadata)
     :jdbc-driver-version (.getDriverVersion metadata)
     :configuration
     {:document-storage :jsonb
      :synchronous-commit (pg-synchronous-commit durability-profile)}}))

(defn- sql-read-doc
  [conn table id]
  (let [row (jdbc/execute-one! conn
                               [(str "SELECT doc FROM " table " WHERE id = ?") id]
                               {:builder-fn rs/as-unqualified-lower-maps})
        doc-json (sql-doc->json (:doc row))]
    (when doc-json
      (decode-json doc-json))))

(defn- pg-idoc-query
  [table spec]
  (case (:type spec)
    :nested       [(str "SELECT id FROM " table
                        " WHERE doc @> ?::jsonb")
                   [(encode-json {:profile {:lang (:value spec)}})]]
    :range        [(str "SELECT id FROM " table
                        " WHERE (doc->'stats'->>'score')::double precision"
                        " > ? AND (doc->'stats'->>'score')::double precision < ?")
                   [(:lo spec) (:hi spec)]]
    :wildcard-one [(str "SELECT id FROM " table
                        " WHERE doc @> ?::jsonb OR doc @> ?::jsonb")
                   [(encode-json {:facts {:city (:value spec)}})
                    (encode-json {:facts {:team (:value spec)}})]]
    :wildcard-depth
    [(str "SELECT id FROM " table
          " WHERE doc @> ?::jsonb")
     [(encode-json {:events [{:entity {:name (:value spec)}}]})]]
    :array        [(str "SELECT id FROM " table
                        " WHERE doc @> ?::jsonb")
                   [(encode-json {:events [{:tags [(:value spec)]}]})]]))

(defn- pg-create-indexes!
  [ds table]
  ;; GIN index for jsonb containment queries
  (jdbc/execute!
    ds [(str "CREATE INDEX IF NOT EXISTS " table "_doc_gin_idx"
             " ON " table " USING GIN (doc jsonb_path_ops)")])
  ;; B-tree indexes for specific paths
  (jdbc/execute!
    ds [(str "CREATE INDEX IF NOT EXISTS " table "_profile_lang_idx"
             " ON " table " ((doc->'profile'->>'lang'))")])
  (jdbc/execute!
    ds [(str "CREATE INDEX IF NOT EXISTS " table "_stats_score_idx"
             " ON " table " (((doc->'stats'->>'score')::double precision))")])
  (jdbc/execute!
    ds [(str "CREATE INDEX IF NOT EXISTS " table "_facts_city_idx"
             " ON " table " ((doc->'facts'->>'city'))")])
  (jdbc/execute!
    ds [(str "CREATE INDEX IF NOT EXISTS " table "_facts_team_idx"
             " ON " table " ((doc->'facts'->>'team'))")]))

(defn- pg-analyze!
  [ds table]
  (jdbc/execute! ds [(str "ANALYZE " table)]))

(defn- sqlite-idoc-query
  [table spec]
  (case (:type spec)
    :nested       [(str "SELECT id FROM " table
                        " WHERE json_extract(doc, '$.profile.lang') = ?")
                   [(:value spec)]]
    :range        [(str "SELECT id FROM " table
                        " WHERE CAST(json_extract(doc, '$.stats.score') AS REAL)"
                        " > ? AND CAST(json_extract(doc, '$.stats.score') AS REAL) < ?")
                   [(:lo spec) (:hi spec)]]
    :wildcard-one [(str "SELECT id FROM " table
                        " WHERE json_extract(doc, '$.facts.city') = ?"
                        " OR json_extract(doc, '$.facts.team') = ?")
                   [(:value spec) (:value spec)]]
    :wildcard-depth
    [(str "SELECT id FROM " table
          " WHERE EXISTS (SELECT 1 FROM json_each(doc, '$.events') e"
          " WHERE json_extract(e.value, '$.entity.name') = ?)")
     [(:value spec)]]
    :array
    [(str "SELECT id FROM " table
          " WHERE EXISTS (SELECT 1 FROM json_each(doc, '$.events') e"
          " JOIN json_each(e.value, '$.tags') t"
          " WHERE t.value = ?)")
     [(:value spec)]]))

(defn- sqlite-create-indexes!
  [conn]
  (jdbc/execute!
    conn [(str "CREATE INDEX IF NOT EXISTS idoc_profile_lang_idx"
               " ON idoc_bench_docs (json_extract(doc, '$.profile.lang'))")])
  (jdbc/execute!
    conn [(str "CREATE INDEX IF NOT EXISTS idoc_stats_score_idx"
               " ON idoc_bench_docs"
               " (CAST(json_extract(doc, '$.stats.score') AS REAL))")])
  (jdbc/execute!
    conn [(str "CREATE INDEX IF NOT EXISTS idoc_facts_city_idx"
               " ON idoc_bench_docs (json_extract(doc, '$.facts.city'))")])
  (jdbc/execute!
    conn [(str "CREATE INDEX IF NOT EXISTS idoc_facts_team_idx"
               " ON idoc_bench_docs (json_extract(doc, '$.facts.team'))")]))

(defn- sqlite-analyze!
  [conn]
  (jdbc/execute! conn ["ANALYZE idoc_bench_docs"]))

(defn- pg-update-stats!
  [conn table id score last-seen]
  (jdbc/execute!
    conn
    [(str "UPDATE " table
          " SET doc = jsonb_set("
          "jsonb_set(doc, '{stats,score}', to_jsonb(?::double precision), true),"
          "'{stats,last_seen}', to_jsonb(?::bigint), true)"
          " WHERE id = ?")
     score
     last-seen
     id]))

(defn- sqlite-update-stats!
  [conn id score last-seen]
  (jdbc/execute!
    conn
    [(str "UPDATE idoc_bench_docs"
          " SET doc = json_set(doc, '$.stats.score', ?, '$.stats.last_seen', ?)"
          " WHERE id = ?")
     score
     last-seen
     id]))

(defn- mongo-filter
  [spec]
  (case (:type spec)
    :nested         (Filters/eq "profile.lang" (:value spec))
    :range          (Filters/and (list
                          (Filters/gt "stats.score" (:lo spec))
                          (Filters/lt "stats.score" (:hi spec))))
    :wildcard-one   (Filters/or (list
                                (Filters/eq "facts.city" (:value spec))
                                (Filters/eq "facts.team" (:value spec))))
    :wildcard-depth (Filters/eq "events.entity.name" (:value spec))
    :array          (Filters/eq "events.tags" (:value spec))))

(defn- mongo-doc
  [id doc]
  (let [bson (Document/parse (encode-json doc))]
    (.put bson "_id" (long id))
    bson))

(defn- bson->clj
  [v]
  (cond
    (instance? java.util.Map v)
    (into {}
          (map (fn [[k val]] [(keyword (str k)) (bson->clj val)]) v))

    (instance? java.util.List v)
    (mapv bson->clj v)

    :else v))

(defn- mongo-read-doc
  [coll id]
  (when-let [doc (.first (.find coll (Filters/eq "_id" (long id))))]
    (dissoc (bson->clj doc) :_id)))

(defn- mongo-query-ids
  [coll spec]
  (into #{}
        (map #(long (.get ^Document % "_id")))
        (.projection (.find coll (mongo-filter spec))
                     (Projections/include (into-array String ["_id"])))))

(defn- mongo-update-stats!
  [coll id score last-seen]
  (let [updates (Document. {"$set" (Document. {"stats.score" score
                                               "stats.last_seen" last-seen})})]
    (.updateOne coll (Filters/eq "_id" (long id)) updates)))

(defn- mongo-create-indexes!
  [coll]
  (.createIndex coll (Document. {"profile.lang" 1}))
  (.createIndex coll (Document. {"stats.score" 1}))
  (.createIndex coll (Document. {"facts.city" 1}))
  (.createIndex coll (Document. {"facts.team" 1}))
  (.createIndex coll (Document. {"events.entity.name" 1}))
  (.createIndex coll (Document. {"events.tags" 1})))

(defn- sqlite-synchronous-mode
  [durability-profile]
  (case durability-profile
    :relaxed ["NORMAL" 1]
    ["FULL" 2]))

(defn- sqlite-pragma
  [conn pragma]
  (first-row-value
    (jdbc/execute-one!
      conn [(str "PRAGMA " pragma ";")]
      {:builder-fn rs/as-unqualified-lower-maps})))

(defn- sqlite-conn
  [ds durability-profile]
  (let [conn                  (jdbc/get-connection ds)
        [sync-mode sync-code] (sqlite-synchronous-mode durability-profile)]
    (try
      (let [journal-mode (-> (jdbc/execute-one!
                               conn ["PRAGMA journal_mode=WAL;"]
                               {:builder-fn rs/as-unqualified-lower-maps})
                             first-row-value
                             str
                             str/lower-case)]
        (when-not (= "wal" journal-mode)
          (throw (ex-info "SQLite did not enter WAL mode"
                          {:expected "wal" :actual journal-mode}))))
      (jdbc/execute! conn [(str "PRAGMA synchronous=" sync-mode ";")])
      (jdbc/execute! conn ["PRAGMA busy_timeout=5000;"])
      (let [actual-sync (long (sqlite-pragma conn "synchronous"))]
        (when-not (= (long sync-code) actual-sync)
          (throw (ex-info "SQLite did not enter the requested durability mode"
                          {:expected sync-code :actual actual-sync}))))
      conn
      (catch Throwable t
        (.close conn)
        (throw t)))))

(defn- sqlite-storage
  [conn durability-profile]
  (let [[sync-mode _] (sqlite-synchronous-mode durability-profile)
        metadata      (.getMetaData conn)]
    {:engine :sqlite
     :sqlite-version (str (first-row-value
                            (jdbc/execute-one!
                              conn ["SELECT sqlite_version() AS version"]
                              {:builder-fn rs/as-unqualified-lower-maps})))
     :jdbc-driver-version (.getDriverVersion metadata)
     :configuration
     {:document-storage :json1-text
      :journal-mode "wal"
      :synchronous (str/lower-case sync-mode)
      :busy-timeout-ms (long (sqlite-pragma conn "busy_timeout"))}}))

(defn- mongo-write-concern
  [durability-profile]
  (case durability-profile
    :relaxed (.withJournal WriteConcern/ACKNOWLEDGED false)
    WriteConcern/JOURNALED))

(defn- mongo-storage
  [database durability-profile]
  (let [build-info (.runCommand database (Document. {"buildInfo" 1}))]
    {:engine :mongodb
     :server-version (.getString build-info "version")
     :driver-version (or (some-> MongoClients .getPackage
                                  .getImplementationVersion)
                         "4.11.1")
     :configuration
     {:document-storage :bson
      :write-concern (if (= :strict durability-profile)
                       {:w 1 :journal true}
                       {:w 1 :journal false})}}))

(defn- datalevin-handlers
  [opts docs]
  (let [{:keys [batch-size env-flags keep-db? idoc-trace?
                dtlv-uri durability-profile]} opts

        dir    (when-not dtlv-uri
                 (or (:dir opts)
                     (str (u/tmp-dir (str "idoc-bench-" (UUID/randomUUID))))))
        db-uri (when dtlv-uri
                 (let [;; Insert credentials if not present
                       uri (if (re-find #"://[^@]+@" dtlv-uri)
                             dtlv-uri
                             (clojure.string/replace-first
                               dtlv-uri #"://" "://datalevin:datalevin@"))]
                   (str uri "/idoc-bench-" (UUID/randomUUID))))
        conn   (if db-uri
                 (d/get-conn db-uri schema)
                 (d/create-conn
                   dir
                   schema
                   {:wal? true
                    :wal-durability-profile durability-profile
                    :kv-opts
                    {:flags (into c/default-env-flags env-flags)}}))
        label  (if db-uri "remote Datalevin" dir)
        trace  (when idoc-trace? (atom {}))
        tracer (fn [spec]
                 (when trace
                   (fn [event] (update-idoc-trace! trace spec event))))
        query-ids
        (fn [spec trace?]
          (let [q   (spec->idoc-query spec)
                db  (d/db conn)
                res (if (and trace trace?)
                      (binding [idoc/*trace* (tracer spec)]
                        (d/q q-idoc db q))
                      (d/q q-idoc db q))]
            (into #{} (map first) res)))]
    {:system       :datalevin
     :label        label
     :storage      {:engine :datalevin-datalog
                    :datalevin-version c/version
                    :native-version (Util/version)
                    :configuration
                    {:document-storage :db.type/idoc
                     :wal? (nil? dtlv-uri)
                     :wal-durability-profile
                     (when-not dtlv-uri durability-profile)
                     :env-flags
                     (when-not dtlv-uri
                       (vec (sort (d/get-env-flags
                                    (d/datalog-kv conn)))))}}
     :load!        (fn []
                     (load-docs-datalevin! conn @docs batch-size)
                     (println "Running analyze ...")
                     (d/analyze (d/db conn)))
     :make-thread  (fn [_] {:conn conn})
     :close-thread (fn [_] nil)
     :op-read      (fn [{:keys [conn]} {:keys [id]}]
                     (let [db (d/db conn)]
                       (:mem/doc (d/entity db id))))
     :op-update    (fn [{:keys [conn]} {:keys [id] :as operation}]
                     (let [idx        (dec id)
                           doc        (nth @docs idx)
                           [doc' ops] (update-doc-patch doc operation)]
                       (swap! docs assoc idx doc')
                       (d/transact! conn [[:db.fn/patchIdoc id :mem/doc ops]])))
     :op-rmw       (fn [{:keys [conn]} {:keys [id] :as operation}]
                     (let [db         (d/db conn)
                           doc        (:mem/doc (d/entity db id))
                           [doc' ops] (rmw-doc-patch doc operation)]
                       (swap! docs assoc (dec id) doc')
                       (d/transact! conn [[:db.fn/patchIdoc id :mem/doc ops]])))
     :op-idoc      (fn [_ {:keys [spec]}] (query-ids spec true))
     :query-ids    (fn [_ spec] (query-ids spec false))
     :close!       (fn [] (d/close conn))
     :trace-report (when trace #(print-idoc-trace trace))
     :trace-data   (when trace #(into {} @trace))
     :reset-trace! (when trace #(reset! trace {}))
     :cleanup!     (fn []
                     (when (and (not keep-db?) dir)
                       (when-not (u/windows?)
                         (u/delete-files dir))))}))

(defn- postgres-handlers
  [opts docs]
  (let [{:keys [batch-size keep-db? pg-url pg-user
                pg-password pg-table durability-profile]} opts

        ds       (jdbc/get-datasource
                   (cond-> {:jdbcUrl pg-url}
                     pg-user     (assoc :user pg-user)
                     pg-password (assoc :password pg-password)))
        storage  (with-open [conn (pg-conn ds durability-profile)]
                   (pg-storage conn durability-profile))
        init!    (fn []
                   (jdbc/execute! ds [(str "DROP TABLE IF EXISTS " pg-table)])
                   (jdbc/execute! ds [(str "CREATE TABLE " pg-table
                                           " (id BIGINT PRIMARY KEY, doc JSONB NOT NULL)")]))
        load!    (fn []
                   (jdbc/with-transaction [tx ds]
                     (doseq [batch (partition-all
                                     batch-size
                                     (map-indexed
                                       (fn [idx doc]
                                         {:id  (inc idx)
                                          :doc (pg-jsonb (encode-json doc))})
                                       @docs))]
                       (sql/insert-multi! tx (keyword pg-table) batch)))
                   (println "Building indexes ...")
                   (pg-create-indexes! ds pg-table)
                   (println "Running ANALYZE ...")
                   (pg-analyze! ds pg-table))
        op-query (fn [conn spec]
                   (let [[sql params] (pg-idoc-query pg-table spec)]
                     (sql-query-ids conn sql params)))
        read-doc (fn [conn id] (sql-read-doc conn pg-table id))
        update-doc!
        (fn [conn id score last-seen]
          (pg-update-stats! conn pg-table id score last-seen))]
    {:system       :postgres
     :label        (str "PostgreSQL (" pg-table ")")
     :storage      storage
     :load!        (fn []
                     (init!)
                     (load!))
     :make-thread  (fn [_] {:conn (pg-conn ds durability-profile)})
     :close-thread (fn [{:keys [conn]}] (.close conn))
     :op-read      (fn [{:keys [conn]} {:keys [id]}]
                     (read-doc conn id))
     :op-update    (fn [{:keys [conn]}
                        {:keys [id score last-seen] :as operation}]
                     (let [idx  (dec id)
                           doc  (nth @docs idx)
                           doc' (update-doc doc operation)]
                       (swap! docs assoc idx doc')
                       (update-doc! conn id score last-seen)))
     :op-rmw       (fn [{:keys [conn]}
                        {:keys [id last-seen] :as operation}]
                     (let [doc  (read-doc conn id)
                           doc' (rmw-doc doc operation)]
                       (swap! docs assoc (dec id) doc')
                       (update-doc! conn id
                                    (get-in doc' [:stats :score])
                                    last-seen)))
     :op-idoc      (fn [{:keys [conn]} {:keys [spec]}]
                     (op-query conn spec))
     :query-ids    (fn [{:keys [conn]} spec] (op-query conn spec))
     :close!       (fn [] nil)
     :cleanup!     (fn []
                     (when-not keep-db?
                       (jdbc/execute!
                         ds [(str "DROP TABLE IF EXISTS " pg-table)])))}))

(defn- sqlite-handlers
  [opts docs]
  (let [{:keys [batch-size keep-db? sqlite-path durability-profile]} opts

        default-dir  (str (u/tmp-dir
                            (str "idoc-bench-sqlite-" (UUID/randomUUID))))
        db-path      (or sqlite-path (str default-dir "/idoc-bench.sqlite"))
        cleanup-dir? (nil? sqlite-path)
        _            (when-let [parent (.getParent (java.io.File. db-path))]
                       (u/file parent))
        ds           (jdbc/get-datasource
                       {:jdbcUrl (str "jdbc:sqlite:" db-path)})
        storage      (with-open [conn (sqlite-conn ds durability-profile)]
                       (sqlite-storage conn durability-profile))
        init!
        (fn []
          (let [conn (sqlite-conn ds durability-profile)]
            (try
              (jdbc/execute! conn [(str "DROP TABLE IF EXISTS idoc_bench_docs")])
              (jdbc/execute!
                conn [(str "CREATE TABLE idoc_bench_docs"
                           " (id INTEGER PRIMARY KEY, doc TEXT NOT NULL)")])
              (finally
                (.close conn)))))
        load!
        (fn []
          (jdbc/with-transaction [tx ds]
            (doseq [batch (partition-all
                            batch-size
                            (map-indexed
                              (fn [idx doc]
                                {:id  (inc idx)
                                 :doc (encode-json doc)})
                              @docs))]
              (sql/insert-multi! tx :idoc_bench_docs batch)))
          (let [conn (sqlite-conn ds durability-profile)]
            (try
              (println "Building indexes ...")
              (sqlite-create-indexes! conn)
              (println "Running ANALYZE ...")
              (sqlite-analyze! conn)
              (finally
                (.close conn)))))
        op-query
        (fn [conn spec]
          (let [[sql params] (sqlite-idoc-query "idoc_bench_docs" spec)]
            (sql-query-ids conn sql params)))
        read-doc     (fn [conn id] (sql-read-doc conn "idoc_bench_docs" id))
        update-doc!
        (fn [conn id score last-seen]
          (sqlite-update-stats! conn id score last-seen))]
    {:system       :sqlite
     :label        db-path
     :storage      storage
     :load!        (fn []
                     (init!)
                     (load!))
     :make-thread  (fn [_] {:conn (sqlite-conn ds durability-profile)})
     :close-thread (fn [{:keys [conn]}] (.close conn))
     :op-read      (fn [{:keys [conn]} {:keys [id]}]
                     (read-doc conn id))
     :op-update    (fn [{:keys [conn]}
                        {:keys [id score last-seen] :as operation}]
                     (let [idx  (dec id)
                           doc  (nth @docs idx)
                           doc' (update-doc doc operation)]
                       (swap! docs assoc idx doc')
                       (update-doc! conn id score last-seen)))
     :op-rmw       (fn [{:keys [conn]}
                        {:keys [id last-seen] :as operation}]
                     (let [doc  (read-doc conn id)
                           doc' (rmw-doc doc operation)]
                       (swap! docs assoc (dec id) doc')
                       (update-doc! conn id
                                    (get-in doc' [:stats :score])
                                    last-seen)))
     :op-idoc      (fn [{:keys [conn]} {:keys [spec]}]
                     (op-query conn spec))
     :query-ids    (fn [{:keys [conn]} spec] (op-query conn spec))
     :close!       (fn [] nil)
     :cleanup!     (fn []
                     (when-not keep-db?
                       (when-not (u/windows?)
                         (u/delete-files db-path)
                         (when (and cleanup-dir?
                                    (u/file-exists default-dir))
                           (u/delete-files default-dir)))))}))

(defn- mongo-handlers
  [opts docs]
  (let [{:keys [batch-size keep-db? mongo-uri mongo-db mongo-coll
                durability-profile]}
        opts

        client   (MongoClients/create mongo-uri)
        database (.getDatabase client mongo-db)
        coll     (.withWriteConcern (.getCollection database mongo-coll)
                                    (mongo-write-concern durability-profile))
        storage  (mongo-storage database durability-profile)]
    {:system       :mongo
     :label        (str "MongoDB (" mongo-db "/" mongo-coll ")")
     :storage      storage
     :load!
     (fn []
       (.drop coll)
       (doseq [batch (partition-all batch-size (map-indexed
                                                 (fn [idx doc]
                                                   (mongo-doc (inc idx) doc))
                                                 @docs))]
         (.insertMany coll (ArrayList. batch)))
       (println "Building indexes ...")
       (mongo-create-indexes! coll))
     :make-thread  (fn [_] {:coll coll})
     :close-thread (fn [_] nil)
     :op-read      (fn [{:keys [coll]} {:keys [id]}]
                     (mongo-read-doc coll id))
     :op-update    (fn [{:keys [coll]}
                        {:keys [id score last-seen] :as operation}]
                     (let [idx  (dec id)
                           doc  (nth @docs idx)
                           doc' (update-doc doc operation)]
                       (swap! docs assoc idx doc')
                       (mongo-update-stats! coll id score last-seen)))
     :op-rmw       (fn [{:keys [coll]}
                        {:keys [id last-seen] :as operation}]
                     (let [doc  (mongo-read-doc coll id)
                           doc' (rmw-doc doc operation)]
                       (swap! docs assoc (dec id) doc')
                       (mongo-update-stats! coll id
                                            (get-in doc' [:stats :score])
                                            last-seen)))
     :op-idoc      (fn [{:keys [coll]} {:keys [spec]}]
                     (mongo-query-ids coll spec))
     :query-ids    (fn [{:keys [coll]} spec]
                     (mongo-query-ids coll spec))
     :close!       (fn [] nil)
     :cleanup!     (fn []
                     (if keep-db?
                       (.close client)
                       (do
                         (.drop coll)
                         (.close client))))}))

(defn- canonical-doc
  [value]
  (cond
    (map? value)
    (into {} (map (fn [[k v]] [(if (keyword? k) (name k) (str k))
                               (canonical-doc v)])) value)

    (sequential? value) (mapv canonical-doc value)
    :else value))

(defn- reference-match?
  [doc {:keys [type value lo hi]}]
  (case type
    :nested (= value (get-in doc [:profile :lang]))
    :range (let [score (get-in doc [:stats :score])]
             (< lo score hi))
    :wildcard-one (boolean (some #{value} (vals (:facts doc))))
    :wildcard-depth
    (boolean (some #(= value (get-in % [:entity :name])) (:events doc)))
    :array
    (boolean (some #(some #{value} (:tags %)) (:events doc)))))

(defn- reference-query-ids
  [docs spec]
  (into #{}
        (keep-indexed (fn [idx doc]
                        (when (reference-match? doc spec) (inc idx))))
        docs))

(defn- verification-specs
  [docs]
  (let [sample-docs (take 8 docs)
        take-values (fn [values] (take 3 (distinct values)))
        nested      (take-values (map #(get-in % [:profile :lang]) sample-docs))
        scores      (take-values (map #(get-in % [:stats :score]) sample-docs))
        facts       (take-values (mapcat (comp vals :facts) sample-docs))
        event-vals  (mapcat :events sample-docs)
        entities'   (take-values (map #(get-in % [:entity :name]) event-vals))
        tags'       (take-values (mapcat :tags event-vals))
        missing     "__idoc_bench_missing__"]
    (vec
      (distinct
        (concat
          (map #(hash-map :type :nested :value %) nested)
          [{:type :nested :value missing}]
          (map #(hash-map :type :range
                          :lo (- (double %) 1e-9)
                          :hi (+ (double %) 1e-9))
               scores)
          [{:type :range :lo 0.25 :hi 0.75}]
          (map #(hash-map :type :wildcard-one :value %) facts)
          [{:type :wildcard-one :value missing}]
          (map #(hash-map :type :wildcard-depth :value %) entities')
          [{:type :wildcard-depth :value missing}]
          (map #(hash-map :type :array :value %) tags')
          [{:type :array :value missing}])))))

(defn- verify-handlers!
  [handlers docs]
  (let [ctx       ((:make-thread handlers) :verification)
        record-n  (count docs)
        read-ids  (vec (distinct [1 (inc (quot (dec record-n) 2)) record-n]))
        specs     (verification-specs docs)]
    (try
      (doseq [id read-ids]
        (let [expected (canonical-doc (nth docs (dec id)))
              actual   (canonical-doc ((:op-read handlers) ctx
                                       {:op :read :id id}))]
          (when-not (= expected actual)
            (throw (ex-info "Point-read correctness check failed"
                            {:system (:system handlers) :id id
                             :expected expected :actual actual})))))
      (let [id          1
            original    (first docs)
            update-op   {:op :update :id id :score 0.123456789
                         :last-seen (+ base-ts 3000001)}
            updated     (update-doc original update-op)
            rmw-op      {:op :rmw :id id :last-seen (+ base-ts 3000002)}
            rmw-updated (rmw-doc updated rmw-op)
            restore-op  {:op :update :id id
                         :score (get-in original [:stats :score])
                         :last-seen (get-in original [:stats :last_seen])}]
        ((:op-update handlers) ctx update-op)
        (when-not (= (canonical-doc updated)
                     (canonical-doc ((:op-read handlers) ctx
                                     {:op :read :id id})))
          (throw (ex-info "Update correctness check failed"
                          {:system (:system handlers) :id id})))
        ((:op-rmw handlers) ctx rmw-op)
        (when-not (= (canonical-doc rmw-updated)
                     (canonical-doc ((:op-read handlers) ctx
                                     {:op :read :id id})))
          (throw (ex-info "Read-modify-write correctness check failed"
                          {:system (:system handlers) :id id})))
        ((:op-update handlers) ctx restore-op))
      (let [query-results
            (mapv
              (fn [spec]
                (let [expected (reference-query-ids docs spec)
                      actual   ((:query-ids handlers) ctx spec)]
                  (when-not (= expected actual)
                    (throw
                      (ex-info "Idoc query correctness check failed"
                               {:system     (:system handlers)
                                :spec       spec
                                :expected-count (count expected)
                                :actual-count   (count actual)
                                :missing    (vec (take 20
                                                       (sort (remove actual
                                                                     expected))))
                                :unexpected (vec (take 20
                                                       (sort (remove expected
                                                                     actual))))})))
                  {:spec spec :result-count (count actual)}))
              specs)]
        (println "Correctness:"
                 (count read-ids) "point reads and"
                 "2 mutations and"
                 (count specs) "idoc queries passed")
        {:status :passed
         :point-read-checks (count read-ids)
         :mutation-checks 2
         :query-checks (count specs)
         :queries query-results})
      (finally
        ((:close-thread handlers) ctx)))))

(defn- build-handlers
  [system opts docs]
  (case system
    :datalevin (datalevin-handlers opts docs)
    :postgres  (postgres-handlers opts docs)
    :sqlite    (sqlite-handlers opts docs)
    :mongo     (mongo-handlers opts docs)
    (throw (ex-info "Unsupported system" {:system system}))))

(defn- execute-operation!
  [handlers ctx record-locks {:keys [op id] :as operation}]
  (let [execute
        (fn []
          (case op
            :read   ((:op-read handlers) ctx operation)
            :update ((:op-update handlers) ctx operation)
            :rmw    ((:op-rmw handlers) ctx operation)
            :idoc   ((:op-idoc handlers) ctx operation)))]
    (if (#{:update :rmw} op)
      (locking (nth record-locks (dec id))
        (execute))
      (execute))))

(defn- run-thread
  [handlers operations tid record-locks]
  (let [ctx ((:make-thread handlers) tid)]
    (try
      (mapv
        (fn [{:keys [op id spec schedule-index] :as operation}]
          (let [start (System/nanoTime)]
            (execute-operation! handlers ctx record-locks operation)
            (cond-> {:thread tid
                     :schedule-index schedule-index
                     :op op
                     :latency-ns (- (System/nanoTime) start)}
              id   (assoc :record-id id)
              spec (assoc :shape (:type spec) :query spec))))
        operations)
      (finally
        ((:close-thread handlers) ctx)))))

(defn- warmup
  [handlers operations record-locks]
  (when (seq operations)
    (let [ctx ((:make-thread handlers) :warmup)]
      (try
        (doseq [operation operations]
          (execute-operation! handlers ctx record-locks operation))
        (finally
          ((:close-thread handlers) ctx))))))

(defn- split-schedule
  [schedule thread-count]
  (let [operation-count (count schedule)
        base            (quot operation-count thread-count)
        extra           (mod operation-count thread-count)]
    (loop [tid    0
           offset 0
           result []]
      (if (= tid thread-count)
        result
        (let [n   (+ base (if (< tid extra) 1 0))
              end (+ offset n)]
          (recur (inc tid) end (conj result (subvec schedule offset end))))))))

(defn- run-workers
  [handlers schedule thread-count record-locks]
  (let [executor (Executors/newFixedThreadPool (int thread-count))
        partitions (split-schedule schedule thread-count)]
    (try
      (let [futures
            (mapv
              (fn [tid operations]
                (.submit executor
                         ^Callable
                         (reify Callable
                           (call [_]
                             (run-thread handlers operations tid record-locks)))))
              (range thread-count)
              partitions)]
        (vec
          (mapcat
            (fn [future]
              (try
                (.get future)
                (catch ExecutionException e
                  (throw (or (.getCause e) e)))))
            futures)))
      (finally
        (.shutdownNow executor)
        (.awaitTermination executor 30 TimeUnit/SECONDS)))))

(defn- timed-call
  [f]
  (let [start (System/nanoTime)
        value (f)]
    [value (/ (- (System/nanoTime) start) 1e6)]))

(defn- run-bench
  [system opts base-docs warmup-schedule schedule]
  (let [docs     (atom (vec base-docs))
        handlers (build-handlers system opts docs)
        {:keys [records ops threads]} opts
        record-locks (vec (repeatedly records #(Object.)))
        warmup-ops (:warmup opts)]
    (println)
    (println "System:" (name system))
    (println "Loading" records "documents into" (:label handlers) "...")
    (try
      (let [[_ load-ms]       (timed-call #((:load! handlers)))
            [correctness verification-ms]
            (timed-call #(if (:verify? opts)
                           (verify-handlers! handlers base-docs)
                           {:status :skipped}))]
        (println "Warmup" warmup-ops "ops ...")
        (let [[_ warmup-ms]
              (timed-call #(warmup handlers warmup-schedule record-locks))]
          (when-let [reset-trace! (:reset-trace! handlers)]
            (reset-trace!))
          (println "Running workload" (:workload opts)
                   "with idoc weight" (:idoc-ratio opts)
                   "on" threads "threads ...")
          (let [start       (System/nanoTime)
                samples     (run-workers handlers schedule threads record-locks)
                elapsed-ms  (/ (- (System/nanoTime) start) 1e6)
                actual-ops  (count samples)
                summary     (summarize-samples samples)
                throughput  (if (pos? elapsed-ms)
                              (/ actual-ops (/ elapsed-ms 1000.0))
                              0.0)]
            (when-not (= ops actual-ops)
              (throw
                (ex-info "Benchmark did not complete every scheduled operation"
                         {:system system :expected ops :actual actual-ops})))
            (print-summary summary actual-ops elapsed-ms)
            (when-let [report (:trace-report handlers)]
              (report))
            {:system system
             :storage (:storage handlers)
             :correctness correctness
             :setup-ms {:load-and-index load-ms
                        :verification verification-ms
                        :warmup warmup-ms}
             :elapsed-ms elapsed-ms
             :throughput-ops-per-second throughput
             :latency-ms summary
             :raw-samples samples
             :idoc-trace (when-let [trace-data (:trace-data handlers)]
                           (trace-data))})))
      (finally
        (try
          ((:close! handlers))
          (finally
            ((:cleanup! handlers))))))))

(defn- summarize-pass
  [results]
  {:aggregation :single-complete-pass
   :systems
   (into
     (sorted-map)
     (map
        (fn [{:keys [system throughput-ops-per-second elapsed-ms latency-ms]}]
          [system
           {:throughput-ops-per-second throughput-ops-per-second
            :elapsed-ms elapsed-ms
            :latency-ms latency-ms}])
        results))})

(defn- print-pass-summary
  [summary]
  (println)
  (println "complete-pass throughput")
  (println "system\tops/sec\telapsed-ms")
  (doseq [[system {:keys [throughput-ops-per-second elapsed-ms]}]
          (get summary :systems)]
    (println
      (str (name system)
           "\t" (format "%.2f" (double throughput-ops-per-second))
           "\t" (format "%.2f" (double elapsed-ms))))))

(def ^:private value-options
  #{"--system" "--workload" "--records" "--ops" "--warmup" "--threads"
    "--batch" "--idoc" "--hotset" "--output" "--dir" "--dtlv-uri"
    "--sqlite-path" "--pg-url" "--pg-user" "--pg-password" "--pg-table"
    "--mongo-uri" "--mongo-db" "--mongo-coll" "--seed" "--durability"
    "--run-role" "--repetitions"})

(defn- require-option-values!
  [args]
  (loop [more args]
    (when-let [arg (first more)]
      (if (contains? value-options arg)
        (do
          (when-not (second more)
            (throw (ex-info (str "Missing value for " arg) {:option arg})))
          (recur (nnext more)))
        (recur (next more))))))

(defn- parse-args
  [args]
  (require-option-values! args)
  (loop [opts default-opts
         more args]
    (if-let [arg (first more)]
      (case arg
        "--system"     (recur (assoc opts :system
                                     (keyword (str/lower-case (second more))))
                              (nnext more))
        "--workload"   (recur (assoc opts :workload
                                     (keyword (str/upper-case (second more))))
                              (nnext more))
        "--records"    (recur (assoc opts :records (Long/parseLong (second more)))
                              (nnext more))
        "--ops"        (recur (assoc opts :ops (Long/parseLong (second more)))
                              (nnext more))
        "--warmup"     (recur (assoc opts :warmup (Long/parseLong (second more)))
                              (nnext more))
        "--threads"    (recur (assoc opts :threads (Long/parseLong (second more)))
                              (nnext more))
        "--batch"      (recur (assoc opts :batch-size (Long/parseLong (second more)))
                              (nnext more))
        "--idoc"       (recur (assoc opts :idoc-ratio (Long/parseLong (second more)))
                              (nnext more))
        "--idoc-trace" (recur (assoc opts :idoc-trace? true) (next more))
        "--no-verify"  (recur (assoc opts :verify? false) (next more))
        "--hotset"     (recur (assoc opts :hotset (Double/parseDouble (second more)))
                              (nnext more))
        "--output"     (recur (assoc opts :output (second more))
                              (nnext more))
        "--dir"        (recur (assoc opts :dir (second more))
                              (nnext more))
        "--dtlv-uri"   (recur (assoc opts :dtlv-uri (second more))
                              (nnext more))
        "--sqlite-path" (recur (assoc opts :sqlite-path (second more))
                               (nnext more))
        "--pg-url"     (recur (assoc opts :pg-url (second more))
                              (nnext more))
        "--pg-user"    (recur (assoc opts :pg-user (second more))
                              (nnext more))
        "--pg-password" (recur (assoc opts :pg-password (second more))
                               (nnext more))
        "--pg-table"   (recur (assoc opts :pg-table (second more))
                              (nnext more))
        "--mongo-uri"  (recur (assoc opts :mongo-uri (second more))
                              (nnext more))
        "--mongo-db"   (recur (assoc opts :mongo-db (second more))
                              (nnext more))
        "--mongo-coll" (recur (assoc opts :mongo-coll (second more))
                              (nnext more))
        "--keep"       (recur (assoc opts :keep-db? true) (next more))
        "--seed"       (recur (assoc opts :seed (Long/parseLong (second more)))
                              (nnext more))
        "--durability" (recur (assoc opts :durability-profile
                                     (keyword (str/lower-case (second more))))
                              (nnext more))
        "--run-role"   (recur (assoc opts :run-role
                                     (keyword (str/lower-case (second more))))
                              (nnext more))
        "--repetitions"
        (throw
          (ex-info
            "--repetitions is not supported; run one warmup JVM pass and one measurement JVM pass"
            {:argument arg :value (second more)}))
        "--help"       (recur (assoc opts :help? true) (next more))
        (throw (ex-info (str "Unrecognized argument: " arg)
                        {:argument arg})))
      opts)))

(defn- usage []
  (println "idoc-bench options:")
  (println "  --system datalevin|postgres|sqlite|mongo|all  (default datalevin)")
  (println "  --workload A|C|F   Workload type (default C)")
  (println "  --records N        Number of documents (default 10000)")
  (println "  --ops N            Number of operations (default 10000)")
  (println "  --warmup N         Warmup ops (default 1000)")
  (println "  --threads N        Number of worker threads (default 1)")
  (println "  --batch N          Load batch size (default 1000)")
  (println "  --idoc N           Weight for idoc queries (default 30)")
  (println "  --idoc-trace       Trace idoc candidate sizes and match stats")
  (println "  --no-verify        Skip correctness checks (not for published results)")
  (println "  --hotset P         Hotset fraction (0-1, default 1.0)")
  (println "  --output PATH      Write host metadata, summaries, and raw samples as EDN")
  (println "  --dir PATH         Datalevin DB directory (default /tmp/idoc-bench-<uuid>)")
  (println "  --dtlv-uri URI     Datalevin server URI (e.g. dtlv://host:port)")
  (println "  --sqlite-path PATH SQLite DB file path")
  (println "  --pg-url URL       Postgres JDBC URL")
  (println "  --pg-user USER     Postgres user")
  (println "  --pg-password PWD  Postgres password")
  (println "  --pg-table NAME    Postgres table (default idoc_bench_docs)")
  (println "  --mongo-uri URI    MongoDB URI")
  (println "  --mongo-db NAME    MongoDB database")
  (println "  --mongo-coll NAME  MongoDB collection")
  (println "  --seed N           RNG seed (default 42)")
  (println "  --durability strict|relaxed  Acknowledgment profile (default strict)")
  (println "  --run-role warmup|measurement  Complete-pass role (default measurement)")
  (println "  --keep             Keep DB artifacts after run")
  (println "  --help             Show this help"))

(def ^:private report-config-keys
  [:system :workload :records :ops :warmup :threads :batch-size
   :idoc-ratio :idoc-trace? :verify? :hotset :seed :durability-profile
   :keep-db?])

(defn- host-info
  []
  {:timestamp  (str (Instant/now))
   :jvm-instance-id jvm-instance-id
   :clojure    (clojure-version)
   :datalevin  c/version
   :datalevin-native (Util/version)
   :java       (System/getProperty "java.version")
   :vm         (System/getProperty "java.vm.name")
   :jvm-arguments (vec (.getInputArguments
                         (ManagementFactory/getRuntimeMXBean)))
   :os         (System/getProperty "os.name")
   :os-version (System/getProperty "os.version")
   :arch       (System/getProperty "os.arch")
   :processors (.availableProcessors (Runtime/getRuntime))
   :benchmark-host-control
   (or (some-> (System/getProperty "idoc.bench.host-control") not-empty)
       (some-> (System/getenv "IDOC_BENCH_HOST_CONTROL") not-empty)
       "unspecified")})

(defn- validate-options
  [{:keys [system workload records ops warmup threads batch-size idoc-ratio
           hotset pg-table durability-profile run-role] :as opts}]
  (cond
    (not (contains? #{:datalevin :postgres :sqlite :mongo :all} system))
    (str "unsupported system: " system)

    (not (contains? workloads workload))
    (str "unsupported workload: " workload)

    (not (pos? records)) "--records must be positive"
    (not (pos? ops)) "--ops must be positive"
    (neg? warmup) "--warmup must not be negative"
    (not (pos? threads)) "--threads must be positive"
    (> threads ops) "--threads must not exceed --ops"
    (not (pos? batch-size)) "--batch must be positive"
    (neg? idoc-ratio) "--idoc must not be negative"
    (not (contains? durability-profiles durability-profile))
    (str "unsupported durability profile: " durability-profile)
    (not (contains? run-roles run-role))
    (str "unsupported run role: " run-role)
    (not (< 0.0 hotset 1.0000000001)) "--hotset must be greater than 0 and at most 1"
    (not (re-matches #"[A-Za-z_][A-Za-z0-9_]*" pg-table))
    "--pg-table must be an unqualified SQL identifier"
    :else opts))

(defn- write-report!
  [path report]
  (let [file (io/file path)]
    (when-let [parent (.getParentFile file)]
      (.mkdirs parent))
    (spit file (with-out-str (pprint/pprint report)))
    (println)
    (println "Wrote results to" (.getPath file))))

(def ^:private comparable-host-keys
  [:clojure :datalevin :datalevin-native :java :vm :jvm-arguments :os
   :os-version :arch :processors :benchmark-host-control])

(defn validate-pass-pair
  "Validate an IDoc warmup/measurement artifact pair. Returns true or throws
   with all protocol mismatches. The two reports must come from distinct JVMs
   and have identical benchmark configuration, schedules, host controls, and
   correctness status."
  [warmup-report measurement-report]
  (let [warmup-role       (get-in warmup-report [:protocol :run-role])
        measurement-role  (get-in measurement-report [:protocol :run-role])
        warmup-jvm        (get-in warmup-report [:host :jvm-instance-id])
        measurement-jvm   (get-in measurement-report [:host :jvm-instance-id])
        correctness-ok?   (fn [report]
                            (every? #(= :passed
                                        (get-in % [:correctness :status]))
                                    (:results report)))
        mismatches
        (cond-> []
          (not= :warmup warmup-role)
          (conj {:field :warmup-role :actual warmup-role})

          (not= :measurement measurement-role)
          (conj {:field :measurement-role :actual measurement-role})

          (or (nil? warmup-jvm)
              (nil? measurement-jvm)
              (= warmup-jvm measurement-jvm))
          (conj {:field :jvm-instance-id
                 :warmup warmup-jvm
                 :measurement measurement-jvm})

          (not= (:configuration warmup-report)
                (:configuration measurement-report))
          (conj {:field :configuration})

          (not= (:schedule warmup-report) (:schedule measurement-report))
          (conj {:field :schedule})

          (not= (mapv :system (:results warmup-report))
                (mapv :system (:results measurement-report)))
          (conj {:field :systems})

          (not= (select-keys (:host warmup-report) comparable-host-keys)
                (select-keys (:host measurement-report) comparable-host-keys))
          (conj {:field :host})

          (not (correctness-ok? warmup-report))
          (conj {:field :warmup-correctness})

          (not (correctness-ok? measurement-report))
          (conj {:field :measurement-correctness}))]
    (when (seq mismatches)
      (throw
        (ex-info "Invalid IDoc benchmark warmup/measurement pair"
                 {:mismatches mismatches})))
    true))

(defn run-benchmark
  [opts]
  (let [validated (validate-options opts)]
    (if (string? validated)
      (throw (ex-info validated
                      {:options (select-keys opts report-config-keys)}))
      (let [all-systems?    (= :all (:system opts))
            systems         (if all-systems?
                              [:datalevin :postgres :sqlite :mongo]
                              [(:system opts)])
            host            (host-info)
            base-docs       (generate-docs (:records opts) (:seed opts))
            warmup-schedule (generate-schedule opts (:warmup opts)
                                               (+ (:seed opts) 1000003))
            schedule        (generate-schedule opts (:ops opts)
                                               (+ (:seed opts) 2000003))
            schedule-info   {:dataset-sha256 (schedule-digest base-docs)
                             :warmup-sha256 (schedule-digest warmup-schedule)
                             :measurement-sha256 (schedule-digest schedule)
                             :operation-counts (frequencies (map :op schedule))
                             :idoc-shape-counts
                             (frequencies
                               (keep #(get-in % [:spec :type]) schedule))
                             :thread-partition :contiguous-static}
            _               (println "Schedule SHA-256:"
                                     (:measurement-sha256 schedule-info))
            _               (println "Run role:" (name (:run-role opts)))
            results         (mapv #(run-bench % opts base-docs
                                              warmup-schedule schedule)
                                  systems)
            summary         (summarize-pass results)
            report          {:format-version 3
                             :benchmark :indexed-document
                             :protocol
                             {:method :two-independent-jvm-passes
                              :run-role (:run-role opts)
                              :retained? (= :measurement (:run-role opts))
                              :same-process-pass-count 1
                              :jvm-instance-id jvm-instance-id}
                             :host host
                             :configuration (select-keys opts report-config-keys)
                             :schedule schedule-info
                             :results results
                             :summary summary}]
        (print-pass-summary summary)
        (when-let [output (:output opts)]
          (write-report! output report))
        report))))

(defn -main
  [& args]
  (try
    (let [opts (parse-args args)]
      (if (:help? opts)
        (usage)
        (run-benchmark opts)))
    (finally
      ;; The benchmark previously left Clojure's future/agent executors alive.
      ;; Keep CLI invocation deterministic even if a dependency starts one.
      (shutdown-agents))))
