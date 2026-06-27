(ns datalevin.jepsen.workload.util
  (:require
   [clojure.string :as str]
   [datalevin.jepsen.local :as local]
   [jepsen.checker :as checker]))

(defn terminal-op?
  [op]
  (contains? #{:ok :fail :info} (:type op)))

(defn indeterminate-exception?
  [e]
  (or (local/transport-failure? e)
      (boolean
       (some
        (fn [cause]
          (let [data (ex-data cause)]
            (or (true? (:indeterminate? data))
                (true? (:indeterminate? (:err-data data))))))
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

(def ^:private retryable-leader-failure-markers
  ["HA write admission rejected"
   "Timed out waiting for durable LSN"
   "Timed out waiting for single leader"
   "Timeout in making request"
   "Unable to connect to server:"
   "Connection refused"])

(def ^:private leader-conn-retry-sleep-ms 250)

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
          (and (string? message)
               (some #(str/includes? message %)
                     retryable-leader-failure-markers))))))

(defn with-retrying-leader-conn
  ([test schema timeout-ms f]
   (with-retrying-leader-conn
     test
     schema
     timeout-ms
     leader-conn-retry-sleep-ms
     f))
  ([test schema timeout-ms retry-sleep-ms f]
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
