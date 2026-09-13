(ns ^:no-doc datalevin.test-adapter
  "Explicit backend dispatch for the shared Clojure regression tests.")

(defprotocol Backend
  (invoke! [backend operation arguments])
  (stop! [backend])
  (backend-info [backend]))

(def ^:dynamic *backend* nil)
(def ^:dynamic *on-unsupported* nil)

(defn unsupported! [origin operation message]
  (throw (ex-info message {::type :unsupported
                          :origin origin :operation operation})))

(defn unsupported-data [error]
  (when (instance? Throwable error)
    (some #(when (= :unsupported (::type (ex-data %))) (ex-data %))
          (take-while some? (iterate ex-cause error)))))

(defonce ^:private default-reference
  (delay ((requiring-resolve 'datalevin.test-adapter.reference/start))))

(defn call!
  "Never retries against another backend. Observe unsupported calls even
  when the original test catches their exceptions."
  [operation arguments]
  (try
    (invoke! (or *backend* @default-reference) operation arguments)
    (catch Throwable error
      (when-let [data (unsupported-data error)]
        (when *on-unsupported* (*on-unsupported* data)))
      (throw error))))

(defn empty-db
  ([] (call! :empty-db []))
  ([dir] (call! :empty-db [dir]))
  ([dir schema] (call! :empty-db [dir schema]))
  ([dir schema opts] (call! :empty-db [dir schema opts])))

(defn db-with [db tx-data] (call! :db-with [db tx-data]))
(defn q [query & inputs] (call! :q (into [query] inputs)))
(defn close-db [db] (call! :close-db [db]))
