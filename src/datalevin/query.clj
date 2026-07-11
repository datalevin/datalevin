(ns ^:no-doc datalevin.query
  "Datalog query entry points."
  (:require
   [datalevin.db :as db]
   [datalevin.query.cache :as qcache]
   [datalevin.query.execute :as qexec]
   [datalevin.query.plan :as qplan]
   [datalevin.query-optimizer :as qo]
   [datalevin.remote :as rt])
  (:import
   [datalevin.db DB]
   [datalevin.parser FindRel]
   [datalevin.remote DatalogStore]))

(defn- apply-limit-offset
  [parsed-q result]
  (if (instance? FindRel (:qfind parsed-q))
    (let [limit  (:qlimit parsed-q)
          offset (:qoffset parsed-q)]
      (->> result
           (#(if offset (drop offset %) %))
           (#(if (or (nil? limit) (= limit -1)) % (take limit %)))))
    result))

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
      (apply-limit-offset parsed-q (qcache/q-result parsed-q inputs)))))

(defn- plan-only
  [q & inputs]
  (with-query-runtime
    (let [parsed-q (qcache/parsed-q q)]
      (qexec/mark-parsing-finished!)
      (qexec/plan* parsed-q inputs))))

(defn- explain*
  [{:keys [run?] :or {run? false}} & args]
  (binding [qplan/*explain*     (volatile! {})
            qcache/*cache?*     false
            qcache/*query-cache* *query-cache*
            qo/*plan-cache*      *plan-cache*
            qplan/*start-time*  (System/nanoTime)]
    (if run?
      (do (apply perform args) @qplan/*explain*)
      (do (apply plan-only args)
          (dissoc @qplan/*explain* :actual-result-size :execution-time)))))

(defn- only-remote-db
  "Return [remote-db [updated-inputs]] if the inputs contain only one db
  and its backing store is a remote one, where the remote-db in the inputs is
  replaced by `:remote-db-placeholder, otherwise return `nil`"
  [inputs]
  (let [dbs (filter db/-searchable? inputs)]
    (when-let [rdb (first dbs)]
      (let [rstore (.-store ^DB rdb)]
        (when (and (= 1 (count dbs))
                   (instance? DatalogStore rstore)
                   (db/db? rdb))
          [rstore (vec (replace {rdb :remote-db-placeholder} inputs))])))))

(defn q
  [query & inputs]
  (if-let [[store inputs'] (only-remote-db inputs)]
    (rt/q store query inputs')
    (apply perform query inputs)))

(defn explain
  [opts query & inputs]
  (if-let [[store inputs'] (only-remote-db inputs)]
    (rt/explain store opts query inputs')
    (apply explain* opts query inputs)))
