(ns ^:no-doc datalevin.query
  "Datalog query entry points."
  (:require
   [datalevin.db :as db]
   [datalevin.prepared :as prepared]
   [datalevin.query.cache :as qcache]
   [datalevin.query.execute :as qexec]
   [datalevin.query.execute.point-lookup :as point]
   [datalevin.query.plan :as qplan]
   [datalevin.query-optimizer :as qo]
   [datalevin.interface :as i]
   [datalevin.util :refer [raise]])
  (:import
   [datalevin.db DB]
   [datalevin.parser BindScalar SrcVar]))

(def ^:dynamic *cache?*
  "Whether query result caching is enabled.

  Kept for compatibility with callers that bind `datalevin.query/*cache?*`;
  the implementation delegates to `datalevin.query.cache/*cache?*`."
  true)

(def ^:dynamic *query-cache*
  "Query parse/result cache, kept for compatibility with older callers."
  qcache/*query-cache*)

(def ^:dynamic *plan-cache*
  "Query plan cache, kept for compatibility with older callers."
  qo/*plan-cache*)

(defmacro ^:private with-query-runtime
  [& body]
  `(binding [qcache/*cache?*      (and qcache/*cache?* *cache?*)
             qcache/*query-cache* *query-cache*
             qo/*plan-cache*      *plan-cache*]
     ~@body))

(defn- perform
  [q & inputs]
  (with-query-runtime
    (let [parsed-q (qcache/parsed-q q)]
      (qexec/mark-parsing-finished!)
      (qcache/q-result parsed-q inputs))))

(defn- plan-only
  [q & inputs]
  (with-query-runtime
    (let [parsed-q (qcache/parsed-q q)]
      (qexec/mark-parsing-finished!)
      (qexec/plan* parsed-q inputs))))

(defn count-plan
  "Count tuples produced by an optimizable where-clause plan without retaining
  its final output. Intended for offline cardinality experiments."
  [query & inputs]
  (with-query-runtime
    (let [parsed-q (qcache/parsed-q query)]
      (qexec/mark-parsing-finished!)
      (qexec/count-plan* parsed-q inputs))))

(defn- explain*
  [{:keys [run? intermediate-counts?]
    :or   {run? false intermediate-counts? true}} & args]
  (binding [qplan/*explain*              (volatile! {})
            qplan/*intermediate-counts?* intermediate-counts?
            qcache/*cache?*              false
            qcache/*query-cache*         *query-cache*
            qo/*plan-cache*              *plan-cache*
            qplan/*start-time*           (System/nanoTime)]
    (if run?
      (do (apply perform args) @qplan/*explain*)
      (do (apply plan-only args)
          (dissoc @qplan/*explain* :actual-result-size :execution-time)))))

(defn- only-remote-db
  "Return [remote-db [updated-inputs]] if the inputs contain only one db
  and its backing store is a remote one, where the remote-db in the inputs is
  replaced by `:remote-db-placeholder, otherwise return `nil`"
  [inputs]
  (loop [remaining (seq inputs) idx 0 remote-db nil remote-idx nil]
    (if remaining
      (let [input (first remaining)]
        (if (db/-searchable? input)
          ;; A local source, or a second DB source, rules out remote execution.
          ;; The usual embedded call exits at its first argument.
          (when (and (nil? remote-db)
                     (db/remote-store? (.-store ^DB input)))
            (recur (next remaining) (inc idx) input idx))
          (recur (next remaining) (inc idx) remote-db remote-idx)))
      (when (and remote-db (db/db? remote-db))
        [(.-store ^DB remote-db)
         (assoc (vec inputs) remote-idx :remote-db-placeholder)]))))

(defn q
  [query & inputs]
  (if-let [[store inputs'] (only-remote-db inputs)]
    (i/q store query inputs')
    (apply perform query inputs)))

(defn- prepared-query [query]
  (let [parsed-q (with-query-runtime (qcache/parsed-q query))
        source (first (:qin parsed-q))]
    (when-not (and (instance? BindScalar source)
                  (instance? SrcVar (:variable source)))
      (raise "A prepared query requires a database source as its first :in binding"
             {:error :prepared/query-inputs}))
    parsed-q))

(defn- check-query-inputs! [inputs expected]
  (when-not (and (vector? inputs) (= expected (count inputs)))
    (raise "Prepared query inputs must be a vector with " expected " values"
           {:error :prepared/query-inputs :expected expected})))

(defn- result-reader [parsed-q]
  (let [execute (qcache/prepare-result-reader parsed-q)
        encoded-execute (point/prepared-executor
                          parsed-q (point/point-lookup-projection-shape parsed-q) true)
        expected (count (:qin parsed-q))]
    (fn [db inputs encoded?]
      (check-query-inputs! inputs expected)
      (let [inputs (if (identical? db (nth inputs 0)) inputs (assoc inputs 0 db))
            result (if encoded? (encoded-execute inputs) point/unsupported)]
        (if (identical? result point/unsupported)
          (with-query-runtime
            (qexec/mark-parsing-finished!)
            (execute inputs))
          result)))))

(defn query-reader
  "Compile reusable result processing and eligible access paths. Sources,
  inputs, cache tokens, deadlines and general plans stay execution-local."
  [query]
  (result-reader (prepared-query query)))

(defn prepare-q
  "Prepare a query bound to a local or remote DB view. Its first :in binding
  must be a database source. Execute with a vector of the remaining inputs."
  [^DB db query]
  {:pre [(db/db? db)]}
  (let [parsed-q (prepared-query query)
        expected (dec (count (:qin parsed-q)))
        store (.-store db)
        remote (when (db/remote-prepared-store? store)
                 (i/prepare-remote-read store :q [query nil]))
        reader (when-not remote (result-reader parsed-q))]
    (prepared/prepared-read
      (fn [inputs]
        (check-query-inputs! inputs expected)
        (if remote
          (do
            (when (some db/-searchable? inputs)
              (raise "Prepared remote queries require exactly one database source"
                     {:error :prepared/query-sources}))
            (remote (into [:remote-db-placeholder] inputs)))
          (reader db (into [db] inputs) false))))))

(defn ^:no-doc q-nested
  "Execute a query invoked by the query-language `q` function without sharing
  the containing query's explain accumulator."
  [query & inputs]
  (binding [qplan/*explain* nil]
    (apply q query inputs)))

(defn explain
  [opts query & inputs]
  (if-let [[store inputs'] (only-remote-db inputs)]
    (i/explain store opts query inputs')
    (apply explain* opts query inputs)))
