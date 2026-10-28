(ns ^:no-doc datalevin.query
  "Datalog query entry points."
  (:require
   [datalevin.db :as db]
   [datalevin.prepared :as prepared]
   [datalevin.query.cache :as qcache]
   [datalevin.query.execute :as qexec]
   [datalevin.query.execute.ordered-range :as ordered-range]
   [datalevin.query.execute.point-lookup :as point]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.query-optimizer :as qo]
   [datalevin.interface :as i]
   [datalevin.util :as u :refer [raise]])
  (:import
   [datalevin.utl QuerySourceDispatch]
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
  ;; binding builds a hash map even for three entries. An array-map literal
  ;; keeps the same binding frame and restoration without that temporary trie.
  `(do
     (push-thread-bindings {#'qcache/*cache?* (and qcache/*cache?* *cache?*)
                            #'qcache/*query-cache* *query-cache*
                            #'qo/*plan-cache* *plan-cache*})
     (try ~@body (finally (pop-thread-bindings)))))

(defmacro ^:private source-inputs? [inputs]
  ;; Keep the primitive boolean branch in each caller; a predicate function
  ;; adds an object-returning Clojure call to native entrypoints.
  `(QuerySourceDispatch/hasSource ~inputs DB))

(defonce ^:private source-executor
  (delay (requiring-resolve 'datalevin.query.source.driver/run)))

(defn- run-source [query inputs options]
  ;; Source execution is JVM-only. Reject before lazy resolution, since a
  ;; native image need not contain the optional executor's compiled classes.
  (when (u/graal?)
    (raise "Query-source execution is unsupported in native images"
           {:error :query-source/unsupported :phase :execution-mode
            :query-id (str (random-uuid))}))
  (@source-executor query inputs options))

(defn- run-native-query
  [parsed-q inputs execute]
  ;; Cache hits use explicit settings. Establish compatibility bindings only
  ;; when planning/execution (and user callbacks) actually need the runtime.
  (with-query-runtime
    (qcache/run-query parsed-q inputs execute)))

(defn- perform
  [q & inputs]
  (let [parsed-q (qcache/parsed-q q *query-cache*)]
    (qexec/mark-parsing-finished!)
    (qcache/q-result parsed-q inputs nil
                    (and qcache/*cache?* *cache?*) run-native-query)))

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
  (when (source-inputs? inputs)
    (raise "Query-source count-plan is unsupported" {:error :query-source/unsupported}))
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
                     (db/remote-store? (.-store ^DB input))
                     (not (db/pending-tx-cache? input)))
            (recur (next remaining) (inc idx) input idx))
          (recur (next remaining) (inc idx) remote-db remote-idx)))
      (when (and remote-db (db/db? remote-db))
        [(.-store ^DB remote-db)
         (assoc (vec inputs) remote-idx :remote-db-placeholder)]))))

(defn- remote-query-result
  ([store query inputs execute] (remote-query-result store query inputs nil execute))
  ([store query inputs validate execute]
   ;; Secondary indexes, server functions and UDF registries can change without
   ;; a source transaction. Their result tokens are only available on the server.
   ;; Inputs can supply index calls through rules or nested queries.
   (if (and *cache?* qcache/*cache?*
            (db/cache-active? store)
            (not-any? #(and (symbol? %)
                           (or (namespace %) (= 'udf %)
                               (contains? qresolve/tuple-producing-fns %)))
                      (tree-seq coll? seq [query inputs])))
     (db/cached-remote-read store [:q query inputs] validate execute)
     (execute))))

(defn q
  [query & inputs]
  (if (source-inputs? inputs)
    (run-source query inputs nil)
    (if-let [[store inputs'] (only-remote-db inputs)]
      (remote-query-result store query inputs' #(i/q store query inputs'))
      (apply perform query inputs))))

(defn- prepared-query [query]
  (let [parsed-q (qcache/parsed-q query *query-cache*)
        source (first (:qin parsed-q))]
    (when-not (and (instance? BindScalar source)
                  (instance? SrcVar (:variable source)))
      (raise "A prepared query requires a database source as its first :in binding"
             {:error :prepared/query-inputs}))
    parsed-q))

(defn- check-query-input-count! [inputs ^long expected]
  (when-not (and (vector? inputs) (== expected (long (count inputs))))
    (raise "Prepared query inputs must be a vector with " expected " values"
           {:error :prepared/query-inputs :expected expected})))

(defn- check-query-inputs! [inputs ^long expected]
  (check-query-input-count! inputs expected)
  (when (source-inputs? inputs)
    (raise "Prepared query sources are unsupported" {:error :query-source/unsupported})))

(defn- result-reader [parsed-q]
  (let [execute (qcache/prepare-result-reader parsed-q run-native-query)
        encoded-point (point/prepared-executor
                        parsed-q (point/point-lookup-projection-shape parsed-q) true)
        encoded-range (ordered-range/prepared-encoded-executor parsed-q)
        expected (count (:qin parsed-q))]
    (fn [db inputs encoded?]
      (check-query-inputs! inputs expected)
      (let [inputs (if (identical? db (nth inputs 0)) inputs (assoc inputs 0 db))
            result (if encoded?
                     (if encoded-range
                       (let [resolved (qexec/resolve-window parsed-q inputs)
                             range-result (encoded-range resolved inputs)]
                         (if (identical? range-result point/unsupported)
                           (encoded-point inputs)
                           range-result))
                       (encoded-point inputs))
                     point/unsupported)]
        (if (identical? result point/unsupported)
          (do
            (qexec/mark-parsing-finished!)
            (execute inputs (and qcache/*cache?* *cache?*)))
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
        remote (when (and (db/remote-prepared-store? store)
                          (not (db/pending-tx-cache? db)))
                 (i/prepare-remote-read store :q [query nil]))
        ;; The local reader checks the assembled inputs before using caches;
        ;; remote preparations check their supplied inputs before the RPC.
        reader (when-not remote (result-reader parsed-q))]
    (if remote
      (prepared/prepared-read
        (fn [inputs]
          (check-query-inputs! inputs expected)
          (when (some db/-searchable? inputs)
            (raise "Prepared remote queries require exactly one database source"
                   {:error :prepared/query-sources}))
          (let [inputs (into [:remote-db-placeholder] inputs)]
            (remote-query-result store query inputs #(db/db? db) #(remote inputs)))))
      (prepared/prepared-read
        (fn [inputs]
          (check-query-input-count! inputs expected)
          (reader db (into [db] inputs) false))))))

(defn ^:no-doc q-nested
  "Execute a query invoked by the query-language `q` function without sharing
  the containing query's explain accumulator."
  [query & inputs]
  (binding [qplan/*explain* nil]
    (apply q query inputs)))

(defn explain
  [opts query & inputs]
  (if (source-inputs? inputs)
    ;; nil is the default explain options map, whereas the source executor
    ;; reserves nil for ordinary query execution.
    (run-source query inputs (if (nil? opts) {} opts))
    (if-let [[store inputs'] (only-remote-db inputs)]
      (i/explain store opts query inputs')
      (apply explain* opts query inputs))))
