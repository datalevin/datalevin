(ns openrulebench.datalevin
  "Datalevin backend for the portable TC, SG, and Join1 task contract.
   Legacy exploratory DBLP/LUBM helpers remain in this namespace, but the
   comparison runner intentionally does not expose them."
  (:require
   [openrulebench.core :as core]
   [openrulebench.data :as data]
   [openrulebench.dblp :as dblp]
   [openrulebench.lubm :as lubm]
   [datalevin.constants :as c]
   [datalevin.core :as d]))

;; =============================================================================
;; Schemas
;; =============================================================================

(def tc-schema
  "Schema for transitive closure benchmark."
  {:edge {:db/valueType   :db.type/ref
          :db/cardinality :db.cardinality/many}})

(def sg-schema
  "Schema for same-generation benchmark.
   OpenRuleBench uses par(X,Y) and sib(X,Y) as base relations."
  {:par {:db/valueType   :db.type/ref
         :db/cardinality :db.cardinality/many}
   :sib {:db/valueType   :db.type/ref
         :db/cardinality :db.cardinality/many}})

(def join1-schema
  "Schema for JOIN1 benchmark.
   Each relation (d1, d2, c2, c3, c4) is a ref attribute."
  {:d1 {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :d2 {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :c2 {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :c3 {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :c4 {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}})

(def dblp-schema
  "Schema for DBLP benchmark (EAV model).
   Each publication attribute is stored as entity-attribute-value triples."
  {:att/attribute {:db/valueType :db.type/keyword}
   :att/value {:db/valueType :db.type/string}})

;; LUBM schema is defined in lubm.clj and referenced directly

;; =============================================================================
;; Rules (OpenRuleBench spec)
;; =============================================================================

;; Transitive Closure Rules
;; tc(A, B) :- par(A, B).
;; tc(A, B) :- par(A, X), tc(X, B).
(def tc-rules
  '[[(tc ?a ?b)
     [?a :edge ?b]]
    [(tc ?a ?b)
     [?a :edge ?x]
     (tc ?x ?b)]])

;; Same Generation Rules (OpenRuleBench spec)
;; sg(X, Y) :- sib(X, Y).
;; sg(X, Y) :- par(X, Z), sg(Z, Z1), par(Y, Z1).
(def sg-rules
  '[;; Base: siblings are same-generation
    [(sg ?x ?y)
     [?x :sib ?y]]
    ;; Recursive: nodes with same-generation par-successors are same-generation
    [(sg ?x ?y)
     [?x :par ?z]
     (sg ?z ?z1)
     [?y :par ?z1]]])

;; JOIN1 Rules (OpenRuleBench spec)
;; 5-way join with intermediate duplicate elimination
;; c1(X, Y) :- d1(X, Z), d2(Z, Y).
;; b2(X, Y) :- c3(X, Z), c4(Z, Y).
;; b1(X, Y) :- c1(X, Z), c2(Z, Y).
;; a(X, Y)  :- b1(X, Z), b2(Z, Y).
(def join1-rules
  '[;; c1 derived from d1 and d2
    [(c1 ?x ?y)
     [?x :d1 ?z]
     [?z :d2 ?y]]
    ;; b2 derived from c3 and c4
    [(b2 ?x ?y)
     [?x :c3 ?z]
     [?z :c4 ?y]]
    ;; b1 derived from c1 and c2
    [(b1 ?x ?y)
     (c1 ?x ?z)
     [?z :c2 ?y]]
    ;; a derived from b1 and b2
    [(a ?x ?y)
     (b1 ?x ?z)
     (b2 ?z ?y)]])

;; =============================================================================
;; Database Setup
;; =============================================================================

(def db-opts
  "Use Datalevin's in-memory KV store for generated benchmark databases."
  {:kv-opts {:inmemory? true}})

(defn create-benchmark-conn
  "Create an in-memory Datalevin connection loaded with benchmark datoms."
  [schema datoms]
  (let [conn (d/create-conn nil schema db-opts)]
    (d/transact! conn datoms)
    conn))

(defn create-tc-db
  "Create a database with edge data for TC benchmark."
  [edges]
  (create-benchmark-conn tc-schema (data/edges->datoms edges)))

(defn create-sg-db
  "Create a database with par/sib data for SG benchmark."
  [relations]
  (create-benchmark-conn sg-schema (data/sg->datoms relations)))

(defn create-join1-db
  "Create a database with JOIN1 relations."
  [relations]
  (create-benchmark-conn join1-schema (data/join1->datoms relations)))

(defn create-dblp-db
  "Create a database with DBLP EAV data."
  [datoms]
  (create-benchmark-conn dblp-schema datoms))

(defn create-lubm-db
  "Create a database with LUBM university data."
  [datoms]
  (create-benchmark-conn lubm/lubm-schema datoms))

;; =============================================================================
;; Query Functions
;; =============================================================================

(defn run-tc-query
  "Transitive closure under a portable binding mode."
  ([db]
   (run-tc-query db :ff 1))
  ([db binding bound]
   (case binding
     :ff (d/q '[:find ?a ?b
                :in $ %
                :where (tc ?a ?b)]
              db tc-rules)
     :bf (d/q '[:find ?b
                :in $ % ?bound
                :where (tc ?bound ?b)]
              db tc-rules bound)
     :fb (d/q '[:find ?a
                :in $ % ?bound
                :where (tc ?a ?bound)]
              db tc-rules bound))))

(defn run-sg-query
  "Same generation under a portable binding mode."
  ([db]
   (run-sg-query db :ff 1))
  ([db binding bound]
   (case binding
     :ff (d/q '[:find ?x ?y
                :in $ %
                :where (sg ?x ?y)]
              db sg-rules)
     :bf (d/q '[:find ?y
                :in $ % ?bound
                :where (sg ?bound ?y)]
              db sg-rules bound)
     :fb (d/q '[:find ?x
                :in $ % ?bound
                :where (sg ?x ?bound)]
              db sg-rules bound))))

(defn run-join1-query
  "Run one of the published JOIN1 predicates and binding modes."
  ([db]
   (run-join1-query db :a :ff 1))
  ([db query binding bound]
   (let [query-form
         (case [query binding]
           [:a :ff]  '[:find ?x ?y :in $ % :where (a ?x ?y)]
           [:a :bf]  '[:find ?y :in $ % ?bound :where (a ?bound ?y)]
           [:a :fb]  '[:find ?x :in $ % ?bound :where (a ?x ?bound)]
           [:b1 :ff] '[:find ?x ?y :in $ % :where (b1 ?x ?y)]
           [:b1 :bf] '[:find ?y :in $ % ?bound :where (b1 ?bound ?y)]
           [:b1 :fb] '[:find ?x :in $ % ?bound :where (b1 ?x ?bound)]
           [:b2 :ff] '[:find ?x ?y :in $ % :where (b2 ?x ?y)]
           [:b2 :bf] '[:find ?y :in $ % ?bound :where (b2 ?bound ?y)]
           [:b2 :fb] '[:find ?x :in $ % ?bound :where (b2 ?x ?bound)])]
     (if (= binding :ff)
       (d/q query-form db join1-rules)
       (d/q query-form db join1-rules bound)))))

(defn run-dblp-query
  "DBLP: 4-way self-join query.
   Finds papers with title, year, author, and month attributes."
  [db]
  (d/q '[:find ?id ?title ?year ?author ?month
         :where
         [?e1 :db/id ?id]
         [?e1 :att/attribute :title]
         [?e1 :att/value ?title]
         [?e2 :db/id ?id]
         [?e2 :att/attribute :year]
         [?e2 :att/value ?year]
         [?e3 :db/id ?id]
         [?e3 :att/attribute :author]
         [?e3 :att/value ?author]
         [?e4 :db/id ?id]
         [?e4 :att/attribute :month]
         [?e4 :att/value ?month]]
       db))

(defn run-lubm-q2
  "LUBM Q2: Find all graduate students (requires type inference)."
  [db]
  (d/q '[:find ?x
         :in $ %
         :where (is-a ?x :GraduateStudent)]
       db lubm/lubm-rules))

(defn run-lubm-q9
  "LUBM Q9: Find students and their advisors."
  [db]
  (d/q '[:find ?x ?advisor
         :in $ %
         :where
         (is-a ?x :Student)
         [?x :advisor ?advisor]]
       db lubm/lubm-rules))

(defn run-lubm-q14
  "LUBM Q14: Find all undergraduate students."
  [db]
  (d/q '[:find ?x
         :where
         [?x :type :UndergraduateStudent]]
       db))

;; =============================================================================
;; Benchmark Runners
;; =============================================================================

(defn run-portable-benchmark
  "Run a parsed TC, SG, or JOIN1 task with setup outside the timed region."
  [{:keys [family binding bound-value query spec] :as task}]
  (let [task-data (core/generate-task-data task)
        conn      (case family
                    :tc (create-tc-db task-data)
                    :sg (create-sg-db task-data)
                    :join1 (create-join1-db task-data))]
    (try
      (let [db (d/db conn)
            _  (d/analyze db)
            _  (System/gc)
            [result time-ms]
            (core/time-once
              (case family
                :tc (run-tc-query db binding bound-value)
                :sg (run-sg-query db binding bound-value)
                :join1 (run-join1-query db query binding bound-value)))]
        {:system "datalevin"
         :benchmark spec
         :time-ms time-ms
         :result-count (count result)
         :base-fact-count (core/task-base-fact-count task task-data)
         :input-digest (core/task-data-digest task task-data)
         :engine-version c/version
         :timing-scope :query-and-materialization
         :status :ok})
      (finally
        (d/close conn)))))

(defn run-tc-benchmark
  "Run TC benchmark on an instance. Returns result map.
   Instance can be: tiny, small, medium, large, xlarge, xxlarge."
  [instance-name]
  (let [instance-key (keyword instance-name)
        edges        (data/generate-tc-instance instance-key)
        conn         (create-tc-db edges)]
    (try
      (let [db (d/db conn)
            _  (d/analyze db)
            _  (System/gc)
            [result time-ms] (core/time-once (run-tc-query db))]
        {:system "datalevin"
         :benchmark (str "tc:" instance-name)
         :time-ms time-ms
         :result-count (count result)
         :status :ok})
      (finally
        (d/close conn)))))

(defn run-sg-benchmark
  "Run SG benchmark on an instance. Returns result map.
   Instance can be: tiny, small, medium, large."
  [instance-name]
  (let [instance-key (keyword instance-name)
        relations    (data/generate-sg-instance instance-key)
        conn         (create-sg-db relations)]
    (try
      (let [db (d/db conn)
            _  (d/analyze db)
            _  (System/gc)
            [result time-ms] (core/time-once (run-sg-query db))]
        {:system "datalevin"
         :benchmark (str "sg:" instance-name)
         :time-ms time-ms
         :result-count (count result)
         :status :ok})
      (finally
        (d/close conn)))))

(defn run-join1-benchmark
  "Run JOIN1 benchmark on an instance. Returns result map.
   Instance can be: small, medium, large (OpenRuleBench)."
  [instance-name]
  (let [instance-key (keyword instance-name)
        relations    (data/generate-join1-instance instance-key)
        conn         (create-join1-db relations)]
    (try
      (let [db (d/db conn)
            _  (d/analyze db)
            _  (System/gc)
            [result time-ms] (core/time-once (run-join1-query db))]
        {:system "datalevin"
         :benchmark (str "join1:" instance-name)
         :time-ms time-ms
         :result-count (count result)
         :status :ok})
      (finally
        (d/close conn)))))

(defn run-dblp-benchmark
  "Run DBLP benchmark on an instance. Returns result map.
   Instance can be: small (2K papers), medium (8K), large (64K)"
  [instance-name]
  (let [instance-key (keyword instance-name)
        _ (when-not (contains? dblp/dblp-instances instance-key)
            (throw (ex-info (str "Unknown DBLP instance: " instance-name)
                            {:available (keys dblp/dblp-instances)})))
        datoms (dblp/load-dblp-instance instance-key)
        conn   (create-dblp-db datoms)]
    (try
      (let [db (d/db conn)
            _  (d/analyze db)
            _  (System/gc)
            [result time-ms] (core/time-once (run-dblp-query db))]
        {:system "datalevin"
         :benchmark (str "dblp:" instance-name)
         :time-ms time-ms
         :result-count (count result)
         :status :ok})
      (finally
        (d/close conn)))))

(defn run-lubm-benchmark
  "Run LUBM benchmark on an instance. Returns result map.
   Instance can be: lubm-1 (1 uni), lubm-10 (10 unis), lubm-50 (50 unis)
   Runs LUBM Q2 (graduate students with type inference)."
  [instance-name]
  (let [instance-key (keyword instance-name)
        _ (when-not (contains? lubm/lubm-instances instance-key)
            (throw (ex-info (str "Unknown LUBM instance: " instance-name)
                            {:available (keys lubm/lubm-instances)})))
        datoms (lubm/load-lubm-instance instance-key)
        conn   (create-lubm-db datoms)]
    (try
      (let [db (d/db conn)
            _  (d/analyze db)
            _  (System/gc)
            ;; Run Q2 as the main benchmark (tests type inference rules)
            [result time-ms] (core/time-once (run-lubm-q2 db))]
        {:system "datalevin"
         :benchmark (str "lubm:" instance-name)
         :time-ms time-ms
         :result-count (count result)
         :status :ok})
      (finally
        (d/close conn)))))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(def default-benchmarks
  "Default benchmarks: benchmark-type:instance-size
   Uses OpenRuleBench standard sizes where available."
  ["tc:50k-cyclic-ff" "sg:6k-cyclic-ff"])

(defn parse-benchmark
  "Parse benchmark spec like 'tc:p1000' into [type instance]."
  [spec]
  (core/parse-benchmark spec))

(defn run-benchmark
  "Run a single benchmark by spec. Returns result map."
  [spec]
  (try
    (run-portable-benchmark (core/require-benchmark-task spec))
    (catch OutOfMemoryError _
      (System/gc)
      {:system "datalevin" :benchmark spec :status :oom})
    (catch Exception e
      (let [oom? (core/out-of-memory? e)]
        (println (if oom? "OOM:" "Error:") (.getMessage e))
        {:system "datalevin"
         :benchmark spec
         :status (if oom? :oom :error)
         :error (.getMessage e)}))))

(defn run-benchmarks
  "Run all specified benchmarks. Returns seq of result maps."
  [benchmark-specs]
  (doall (map run-benchmark benchmark-specs)))

(defn -main [& args]
  (let [report (try
                 (core/run-system-cli! "datalevin" default-benchmarks
                                       run-benchmark args)
                 (finally
                   (shutdown-agents)))]
    (System/exit (:exit-code report))))

(comment
  ;; Quick test with generated data
  (def edges (data/generate-tc-instance :small))
  (count edges) ;; => 50000

  (def conn (create-tc-db edges))
  (def db (d/db conn))
  (time (count (run-tc-query db)))
  (d/close conn)

  ;; Run OpenRuleBench benchmarks
  (run-tc-benchmark "small")
  (run-sg-benchmark "small")
  (run-join1-benchmark "small")

  )
