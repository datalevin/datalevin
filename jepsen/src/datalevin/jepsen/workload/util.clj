(ns datalevin.jepsen.workload.util
  (:require
   [clojure.string :as str]
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [jepsen.checker :as checker]))

(defn terminal-op?
  [op]
  (contains? #{:ok :fail :info} (:type op)))

(def ^:private indeterminate-error-codes
  #{:ha/client-op-timeout
    :ha/control-timeout
    :ha/write-indeterminate})

(def ^:private indeterminate-error-types
  #{:txlog/commit-timeout})

(def ^:private indeterminate-error-markers
  ["HA control command timed out"
   "HA write commit confirmation failed"
   "Timed out waiting for durable LSN"
   "Timed out waiting for HA client op replay"])

(defn- indeterminate-error-data?
  [data]
  (boolean
    (or (true? (:indeterminate? data))
        (contains? indeterminate-error-codes (:error data))
        (contains? indeterminate-error-types (:type data)))))

(defn- indeterminate-error-message?
  [message]
  (and (string? message)
       (boolean
         (some #(str/includes? message %)
               indeterminate-error-markers))))

(defn indeterminate-exception?
  [e]
  (or (local/transport-failure? e)
      (boolean
       (some
        (fn [cause]
          (let [data (ex-data cause)]
            (or (indeterminate-error-data? data)
                (indeterminate-error-data? (:err-data data))
                (indeterminate-error-message? (ex-message cause)))))
        (take-while some? (iterate ex-cause e))))))

(defn exception-result-type
  [e]
  (if (indeterminate-exception? e)
    :info
    :fail))

(def ^:private history-safe-max-depth 16)
(def ^:private history-safe-max-items 64)

(defn- primitive-history-value?
  [x]
  (or (nil? x)
      (string? x)
      (keyword? x)
      (symbol? x)
      (number? x)
      (boolean? x)
      (char? x)
      (uuid? x)))

(defn- class-name
  [x]
  (when (some? x)
    (.getName (class x))))

(defn- truncation-summary
  [reason x]
  {:datalevin.jepsen/truncated? true
   :datalevin.jepsen/reason reason
   :datalevin.jepsen/class (class-name x)})

(defn- collection-truncation-summary
  [x]
  (assoc (truncation-summary :collection-limit x)
         :datalevin.jepsen/limit history-safe-max-items))

(defn- safe-opaque-string
  [x]
  (try
    (str x)
    (catch Throwable e
      (str "#<"
           (class-name x)
           " threw while stringifying: "
           (or (ex-message e) (class-name e))
           ">"))))

(defn- limited-items
  [xs]
  (let [limit (long history-safe-max-items)
        items (doall (take (unchecked-inc limit) xs))]
    [(take limit items)
     (> (long (count items)) limit)]))

(defn history-safe
  "Returns x as data Jepsen can persist in history files.

  Diagnostic maps can include runtime objects captured in ex-data. Fressian
  cannot encode those, so keep normal EDN-ish values and stringify opaque
  handles at the edges."
  ([x]
   (history-safe x history-safe-max-depth))
  ([x depth]
   (let [depth (long depth)]
     (cond
       (primitive-history-value? x)
       x

       (neg? depth)
       (truncation-summary :max-depth x)

       (instance? Throwable x)
       (cond-> {:message (or (ex-message x)
                             (.getName (class x)))
                :class (.getName (class x))}
         (some? (ex-data x))
         (assoc :data (history-safe (ex-data x) (unchecked-dec depth))))

       (map-entry? x)
       (clojure.lang.MapEntry.
        (history-safe (key x) (unchecked-dec depth))
        (history-safe (val x) (unchecked-dec depth)))

       (map? x)
       (let [[items truncated?] (limited-items x)]
         (cond-> (into {}
                       (map (fn [[k v]]
                              [(history-safe k (unchecked-dec depth))
                               (history-safe v (unchecked-dec depth))]))
                       items)
           truncated?
           (assoc :datalevin.jepsen/truncated
                  (collection-truncation-summary x))))

       (sequential? x)
       (let [[items truncated?] (limited-items x)]
         (cond-> (mapv #(history-safe % (unchecked-dec depth)) items)
           truncated?
           (conj (collection-truncation-summary x))))

       (set? x)
       (let [[items truncated?] (limited-items x)]
         (cond-> (set (map #(history-safe % (unchecked-dec depth)) items))
           truncated?
           (conj (collection-truncation-summary x))))

       :else
       (safe-opaque-string x)))))

(defn exception-detail
  [e]
  (cond-> {:message (or (ex-message e)
                        (.getName (class e)))
           :class (.getName (class e))}
    (map? (ex-data e))
    (merge (history-safe (ex-data e)))))

(defn assoc-exception-op
  ([op e error]
   (assoc-exception-op op e error nil))
  ([op e error detail]
   (cond-> (assoc op
                  :type (exception-result-type e)
                  :error error)
     (some? detail)
     (assoc :value detail))))

(def ^:private cached-leader-conn-key
  ::cached-leader-conn)

(def ^:private stale-cached-leader-conn-markers
  ["HA write admission rejected"
   "HA read admission rejected"
   "Replica is read-only"
   "This client is closed"
   "Connection is closed"
   "Timeout in making request"
   "Unable to connect to server:"
   "Connection refused"])

(def ^:dynamic *open-leader-conn*
  local/open-leader-conn!)

(defn attach-cached-leader-conn
  [client]
  (assoc client cached-leader-conn-key (atom nil)))

(defn- close-conn!
  [conn]
  (when conn
    (try
      (d/close conn)
      (catch Throwable _
        nil))))

(defn close-cached-leader-conn!
  [client]
  (when-let [conn* (get client cached-leader-conn-key)]
    (when-let [conn @conn*]
      (reset! conn* nil)
      (close-conn! conn))))

(defn- stale-cached-leader-conn-error?
  [e]
  (let [data     (ex-data e)
        err-data (or (:err-data data) data)
        message  (ex-message e)]
    (boolean
      (or (local/transport-failure? e)
          (contains? #{:ha/write-rejected
                       :ha/read-rejected
                       :ha/pinned-request-failed}
                     (:error err-data))
          (and (string? message)
               (some #(str/includes? message %)
                     stale-cached-leader-conn-markers))))))

(defn- cached-leader-conn!
  [conn* test schema]
  (or @conn*
      (locking conn*
        (or @conn*
            (let [conn (*open-leader-conn* test schema)]
              (reset! conn* conn)
              conn)))))

(defn with-cached-leader-conn
  "Runs f with a per-client leader connection.

  The operation body is never retried. If a cached connection appears stale, it
  is closed and cleared for the next operation, while the current operation's
  exception is still reported to Jepsen."
  [client test schema f]
  (let [conn* (or (get client cached-leader-conn-key)
                  (throw (ex-info
                          "Jepsen client is missing cached leader connection state"
                          {})))]
    (try
      (f (cached-leader-conn! conn* test schema))
      (catch Throwable e
        (when (stale-cached-leader-conn-error? e)
          (close-cached-leader-conn! client))
        (throw e)))))

(def ^:private retryable-leader-failure-markers
  ["HA write admission rejected"
   "HA read admission rejected"
   "Timed out waiting for durable LSN"
   "Timed out waiting for single leader"
   "Timeout in making request"
   "Unable to connect to server:"
   "Connection refused"])

(def ^:private leader-conn-retry-sleep-ms 250)

(def ^:private retrying-leader-conn-purposes
  #{:setup :bootstrap})

(def ^:dynamic *with-leader-conn* local/with-leader-conn)

(defn retryable-leader-conn-error?
  [e]
  (let [err-data (or (:err-data (ex-data e))
                     (ex-data e))
        message  (ex-message e)]
    (boolean
      (or (local/transport-failure? e)
          (true? (:retryable? err-data))
          (= :txlog/commit-timeout (:type err-data))
          (= :ha/write-rejected (:error err-data))
          (= :ha/write-indeterminate (:error err-data))
          (and (string? message)
               (some #(str/includes? message %)
                     retryable-leader-failure-markers))))))

(defn- valid-retrying-leader-conn-purpose!
  [purpose]
  (when-not (contains? retrying-leader-conn-purposes purpose)
    (throw (ex-info
             (str "with-retrying-leader-conn is only for idempotent "
                  "setup/bootstrap paths; do not use it from Jepsen "
                  "operation invoke paths")
             {:purpose purpose
              :allowed-purposes retrying-leader-conn-purposes})))
  purpose)

(defn with-retrying-leader-conn
  "Retries broad HA/transport leader-connection failures while running f.

  This helper deliberately accepts only idempotent setup/bootstrap purposes.
  Do not use it for client operation paths: a substring-classified timeout can
  be an indeterminate write, and retrying that write can double-apply it."
  ([_test _schema _timeout-ms _f]
   (throw (ex-info
            (str "with-retrying-leader-conn requires an explicit :setup "
                 "or :bootstrap purpose")
            {:allowed-purposes retrying-leader-conn-purposes})))
  ([purpose test schema timeout-ms f]
   (with-retrying-leader-conn
     purpose
     test
     schema
     timeout-ms
     leader-conn-retry-sleep-ms
     f))
  ([purpose test schema timeout-ms retry-sleep-ms f]
   (valid-retrying-leader-conn-purpose! purpose)
   (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
     (loop []
       (let [result (try
                      {:ok? true
                       :value (*with-leader-conn* test schema f)}
                      (catch Throwable e
                        {:ok? false
                         :error e}))]
         (if (:ok? result)
           (:value result)
           (let [e (:error result)]
             (if (and (< (System/currentTimeMillis) deadline)
                      (retryable-leader-conn-error? e))
               (do
                 (Thread/sleep (long retry-sleep-ms))
                 (recur))
               (throw e)))))))))

(defn tx-report-db
  [conn report]
  (or (:db-after report)
      (:db-before report)
      @conn))

(defn expected-disruption-failures
  [test history pred]
  (->> history
       (filter pred)
       (filter terminal-op?)
       (filter (fn [{:keys [error]}]
                 (local/expected-disruption-write-failure? test error)))
       vec))

(defn disruption-failure-samples
  [ops sample-keys]
  (vec (take 10
             (map #(select-keys % sample-keys) ops))))

(defn read-only-micro-op-txn?
  [op]
  (and (terminal-op? op)
       (sequential? (:value op))
       (seq (:value op))
       (every? (fn [micro-op]
                 (= :r (first micro-op)))
               (:value op))))

(defn append-only-micro-op-txn?
  [op]
  (and (terminal-op? op)
       (sequential? (:value op))
       (seq (:value op))
       (every? (fn [micro-op]
                 (= :append (first micro-op)))
               (:value op))))

(defn append-graph-ignorable-micro-op-txn?
  [op]
  (or (read-only-micro-op-txn? op)
      (append-only-micro-op-txn? op)))

(defn wrap-empty-graph-checker
  ([base-checker pred sample-keys]
   (wrap-empty-graph-checker base-checker
                             pred
                             sample-keys
                             (constantly false)))
  ([base-checker pred sample-keys ignorable-terminal?]
   (reify checker/Checker
     (check [_ test history opts]
       (let [result               (checker/check base-checker test history opts)
             disruption-failures  (expected-disruption-failures test history pred)
             terminal             (->> history
                                       (filter pred)
                                       (filter terminal-op?))
             checked-terminal     (remove (fn [op]
                                            (or ((set disruption-failures) op)
                                                (ignorable-terminal? op)))
                                          terminal)
             empty-graph?         (true? (get-in result
                                                 [:anomalies
                                                  :empty-transaction-graph]))
             only-ignorable?      (and (= :unknown (:valid? result))
                                       empty-graph?
                                       (pos? (count terminal))
                                       (empty? checked-terminal))
             only-disruption?     (and (= :unknown (:valid? result))
                                       empty-graph?
                                       (pos? (count disruption-failures))
                                       (empty? checked-terminal))
             failure-summary      {:disruption-failure-count
                                   (count disruption-failures)
                                   :disruption-failure-samples
                                   (disruption-failure-samples
                                     disruption-failures
                                     sample-keys)}]
         (cond-> (merge result failure-summary)
           only-ignorable?
           (assoc :base-valid? (:valid? result)
                  :adjusted-valid? :ignorable-empty-graph)

           only-disruption?
           (assoc :base-valid? (:valid? result)
                  :adjusted-valid? :disruption-only-empty-graph)))))))
