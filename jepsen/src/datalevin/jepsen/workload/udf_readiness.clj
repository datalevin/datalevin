(ns datalevin.jepsen.workload.udf-readiness
  (:require
   [clojure.string :as str]
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [datalevin.udf :as udf]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [jepsen.generator :as gen]))

(def schema
  {:counter/id    {:db/valueType :db.type/keyword
                   :db/unique :db.unique/identity}
   :counter/value {:db/valueType :db.type/long}})

(def ^:private descriptor
  {:udf/lang :test
   :udf/kind :tx-fn
   :udf/id   :counter/inc})

(def ^:private counter-ident :main)
(def ^:private counter-entity-id "counter-main")
(def ^:private tx-fn-entity-id "counter-inc-tx-fn")
(def ^:private initial-value 0)
(def ^:private expected-value 1)
(def ^:private default-setup-timeout-ms 15000)
(def ^:private converge-timeout-ms 30000)
(def ^:private leader-conn-retry-sleep-ms 250)
(def ^:private sample-limit 10)
(def ^:private default-nodes ["n1" "n2" "n3"])
(def ^:private retryable-leader-failure-markers
  ["HA write admission rejected"
   "Timed out waiting for single leader"
   "Timeout in making request"
   "Unable to connect to server:"
   "Connection refused"])
(def ^:private counter-value-query
  '[:find ?value .
    :in $ ?counter-id
    :where
    [?e :counter/id ?counter-id]
    [?e :counter/value ?value]])

(defonce ^:private registries-by-db-name (atom {}))
(defonce ^:private udf-ready-db-names (atom #{}))
(defonce ^:private initialized-clusters (atom #{}))
(defonce ^:private scenario-runs (atom {}))

(defn- registry-for-db-name
  [db-name]
  (or (get @registries-by-db-name db-name)
      (let [registry (udf/create-registry)]
        (get (swap! registries-by-db-name
                    (fn [registries]
                      (if (contains? registries db-name)
                        registries
                        (assoc registries db-name registry))))
             db-name))))

(defn- enable-udf-readiness!
  [test]
  (swap! udf-ready-db-names conj (:db-name test)))

(defn server-runtime-opts-override
  [_server db-name _store _m]
  {:ha-require-udf-ready? (contains? @udf-ready-db-names db-name)
   :udf-registry (registry-for-db-name db-name)})

(defn- counter-tx-fn
  [db counter-id]
  (if-let [[eid value] (first (d/q '{:find  [?e ?value]
                                     :in    [$ ?counter-id]
                                     :where [[?e :counter/id ?counter-id]
                                             [?e :counter/value ?value]]}
                                   db counter-id))]
    [{:db/id eid
      :counter/value (inc (long value))}]
    []))

(defn- node-counter-value
  [test logical-node]
  (let [value (local/local-query (:datalevin/cluster-id test)
                                 logical-node
                                 counter-value-query
                                 counter-ident)]
    (when-not (= ::local/unavailable value)
      (some-> value long))))

(defn- wait-for-counter-values-on-nodes!
  [test logical-nodes expected-value timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [last-snapshot nil]
      (let [snapshot (into {}
                           (map (fn [logical-node]
                                  [logical-node
                                   (node-counter-value test logical-node)]))
                           logical-nodes)]
        (cond
          (every? (fn [[_ value]]
                    (and (some? value)
                         (= (long expected-value) (long value))))
                  snapshot)
          snapshot

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info "Timed out waiting for UDF-readiness counter convergence"
                          {:logical-nodes logical-nodes
                           :timeout-ms timeout-ms
                           :expected-value (long expected-value)
                           :snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn- wait-for-initial-counter!
  [test]
  (wait-for-counter-values-on-nodes!
   test
   (-> (:nodes test) sort vec)
   initial-value
   (local/workload-setup-timeout-ms (:datalevin/cluster-id test)
                                    default-setup-timeout-ms)))

(defn- ensure-udf-installed!
  [test]
  (let [cluster-id (:datalevin/cluster-id test)
        registry   (registry-for-db-name (:db-name test))]
    (when-not (contains? @initialized-clusters cluster-id)
      (locking initialized-clusters
        (when-not (contains? @initialized-clusters cluster-id)
          (local/with-leader-conn
            test
            schema
            (fn [conn]
              (d/transact! conn
                           [{:db/id counter-entity-id
                             :counter/id counter-ident
                             :counter/value (long initial-value)}
                            {:db/id tx-fn-entity-id
                             :db/ident :counter/inc
                             :db/udf descriptor}])))
          (let [leader-node (:leader (local/wait-for-single-leader!
                                      cluster-id
                                      converge-timeout-ms))
                target-lsn  (local/effective-local-lsn cluster-id leader-node)]
            (local/wait-for-live-nodes-at-least-lsn!
             cluster-id
             target-lsn
             converge-timeout-ms))
          (wait-for-initial-counter! test)
          (enable-udf-readiness! test)
          (swap! initialized-clusters conj cluster-id))))))

(defn- invoke-tx-fn!
  [test]
  (local/with-leader-conn
    test
    schema
    (fn [conn]
      (d/transact! conn [[:db.fn/call descriptor counter-ident]]))))

(defn- retryable-leader-conn-error?
  [e]
  (let [err-data (or (:err-data (ex-data e))
                     (ex-data e))
        message  (ex-message e)]
    (or (local/transport-failure? e)
        (true? (:retryable? err-data))
        (= :ha/write-rejected (:error err-data))
        (and (string? message)
             (some #(str/includes? message %)
                   retryable-leader-failure-markers)))))

(defn- invoke-tx-fn-with-retry!
  [test timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop []
      (let [result (try
                     {:ok? true
                      :value (invoke-tx-fn! test)}
                     (catch Throwable e
                       {:ok? false
                        :error e}))]
        (if (:ok? result)
          (:value result)
          (let [e (:error result)]
            (if (and (< (System/currentTimeMillis) deadline)
                     (retryable-leader-conn-error? e))
              (do
                (Thread/sleep (long leader-conn-retry-sleep-ms))
                (recur))
              (throw e))))))))

(defn- normalize-error-data
  [e]
  (let [data     (ex-data e)
        err-data (or (:err-data data) data)]
    (cond-> (if (map? err-data)
              err-data
              {:message (or (ex-message e)
                            (.getName (class e)))})
      (:server-message data)
      (assoc :server-message (:server-message data))

      (nil? (:message err-data))
      (assoc :message (or (ex-message e)
                          (.getName (class e)))))))

(defn- scenario-op
  []
  {:type :invoke
   :f :exercise})

(defn- run-scenario!
  [test]
  (let [registry (registry-for-db-name (:db-name test))]
    (ensure-udf-installed! test)
  (let [cluster-id      (:datalevin/cluster-id test)
        live-nodes      (-> (local/cluster-state cluster-id)
                            :live-nodes
                            sort
                            vec)
        leader-before   (:leader (local/wait-for-single-leader!
                                  cluster-id
                                  converge-timeout-ms))
        _               (wait-for-counter-values-on-nodes!
                         test
                         live-nodes
                         initial-value
                         converge-timeout-ms)
        failed-error    (try
                          (invoke-tx-fn! test)
                          (throw (ex-info
                                  "UDF-readiness write unexpectedly succeeded"
                                  {:cluster-id cluster-id
                                   :leader-before leader-before}))
                          (catch Throwable e
                            (normalize-error-data e)))
        _               (udf/register! registry descriptor counter-tx-fn)
        _               (invoke-tx-fn-with-retry! test converge-timeout-ms)
        leader-after    (:leader (local/wait-for-single-leader!
                                  cluster-id
                                  converge-timeout-ms))
        target-lsn      (local/effective-local-lsn cluster-id leader-after)
        _               (local/wait-for-nodes-at-least-lsn!
                         cluster-id
                         live-nodes
                         target-lsn
                         converge-timeout-ms)
        nodes           (wait-for-counter-values-on-nodes!
                         test
                         live-nodes
                         expected-value
                         converge-timeout-ms)]
    {:leader-before leader-before
     :leader-after leader-after
     :live-nodes live-nodes
     :failed-error failed-error
     :nodes (into {}
                  (map (fn [[logical-node value]]
                         [logical-node {:value value}]))
                  nodes)})))

(defn- scenario-result
  [test]
  (let [cluster-id (:datalevin/cluster-id test)
        [result-promise owner?]
        (locking scenario-runs
          (if-let [p (get @scenario-runs cluster-id)]
            [p false]
            (let [p (promise)]
              (swap! scenario-runs assoc cluster-id p)
              [p true])))]
    (when owner?
      (try
        (deliver result-promise {:type :ok
                                 :value (run-scenario! test)})
        (catch Throwable e
          (deliver result-promise {:type :error
                                   :error e}))))
    (let [{:keys [type value error]} @result-promise]
      (case type
        :ok value
        (throw error)))))

(defn- checker*
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [oks (->> history
                     (filter (fn [{:keys [type f value]}]
                               (and (= :ok type)
                                    (= :exercise f)
                                    (map? value))))
                     (map :value)
                     vec)
            failures (->> history
                          (filter (fn [{:keys [type f]}]
                                    (and (= :exercise f)
                                         (#{:fail :info} type))))
                          (mapv (fn [{:keys [type error value]}]
                                  {:type type
                                   :error error
                                   :value value})))
            missing-rejection
            (->> oks
                 (remove :failed-error)
                 vec)
            mismatches
            (->> oks
                 (mapcat (fn [{:keys [nodes]}]
                           (keep (fn [[logical-node {:keys [value]}]]
                                   (when (or (nil? value)
                                             (not= (long expected-value)
                                                   (long value)))
                                     {:logical-node logical-node
                                      :expected (long expected-value)
                                      :actual value}))
                                 nodes)))
                 vec)]
        {:valid? (boolean (and (seq oks)
                               (empty? failures)
                               (empty? missing-rejection)
                               (empty? mismatches)))
         :exercise-count (count oks)
         :failure-count (count failures)
         :failure-samples (vec (take sample-limit failures))
         :missing-rejection-count (count missing-rejection)
         :missing-rejection-samples
         (vec (take sample-limit
                    (map #(select-keys % [:failed-error])
                         missing-rejection)))
         :mismatch-count (count mismatches)
         :mismatch-samples (vec (take sample-limit mismatches))}))))

(defrecord Client [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [this test]
    (ensure-udf-installed! test)
    this)

  (invoke! [this test op]
    (try
      (ensure-udf-installed! test)
      (case (:f op)
        :exercise
        (assoc op
               :type :ok
               :value (scenario-result test))

        (assoc op
               :type :fail
               :error [:unsupported-client-op (:f op)]))
      (catch Throwable e
        (assoc op
               :type :fail
               :error (or (ex-message e)
                          (.getName (class e)))
               :value (cond-> {:message (ex-message e)
                               :class (.getName (class e))}
                        (map? (ex-data e))
                        (merge (ex-data e)))))))

  (teardown! [this _test]
    this)

  (close! [_this _test]
    nil))

(defn workload
  [_opts]
  {:client (->Client nil)
   :generator (gen/once (scenario-op))
   :checker (checker*)
   :schema schema
   :nodes default-nodes
   :datalevin/server-runtime-opts-fn
   'datalevin.jepsen.workload.udf-readiness/server-runtime-opts-override})
