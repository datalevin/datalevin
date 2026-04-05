(ns datalevin.jepsen.workload.giant-values
  (:require
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.workload.util :as workload.util]
   [jepsen.checker :as checker]
   [jepsen.checker.timeline :as timeline]
   [jepsen.client :as client]
   [jepsen.generator :as gen]
   [jepsen.independent :as independent]
   [knossos.model :as model]))

(def schema
  {:giant/key     {:db/valueType :db.type/long
                   :db/unique :db.unique/identity}
   :giant/version {:db/valueType :db.type/long}
   :giant/payload {:db/valueType :db.type/string}})

(def ^:private initial-value 0)
(def ^:private default-payload-bytes 12000)
(def ^:private default-setup-timeout-ms 15000)
(defonce ^:private initialized-clusters (atom #{}))
(def ^:private giant-rows-query
  '[:find ?key ?version ?payload
    :where
    [?e :giant/key ?key]
    [?e :giant/version ?version]
    [?e :giant/payload ?payload]])

(def ^:private giant-state-query
  '[:find ?e ?version ?payload
    :in $ ?key
    :where
    [?e :giant/key ?key]
    [?e :giant/version ?version]
    [?e :giant/payload ?payload]])

(defn- payload-for
  [k version payload-bytes]
  (let [payload-bytes (long payload-bytes)
        prefix        (str "giant|" (long k) "|" (long version) "|")
        filler        (str "payload-" (long k) "-" (long version) "-")
        sb            (StringBuilder. prefix)]
    (when (< payload-bytes (count prefix))
      (throw (ex-info "giant payload size is smaller than payload prefix"
                      {:payload-bytes payload-bytes
                       :prefix-bytes  (count prefix)})))
    (while (< (.length sb) payload-bytes)
      (.append sb filler))
    (.substring (.toString sb) 0 (int payload-bytes))))

(defn- giant-entity
  [k version payload-bytes]
  {:db/id         (str "giant-" (long k))
   :giant/key     (long k)
   :giant/version (long version)
   :giant/payload (payload-for k version payload-bytes)})

(defn- write-op
  [_ _]
  {:type :invoke
   :f :write
   :value (rand-int 32)})

(defn- read-op
  [_ _]
  {:type :invoke
   :f :read})

(defn- cas-op
  [_ _]
  {:type :invoke
   :f :cas
   :value [(rand-int 32) (rand-int 32)]})

(defn- giant-state
  [db payload-bytes k]
  (if-some [[_ version payload] (first (d/q giant-state-query db (long k)))]
    (let [version        (some-> version long)
          expected       (when (some? version)
                           (payload-for k version payload-bytes))
          payload-valid? (and (string? payload)
                              (some? expected)
                              (= expected payload))]
      {:present?       true
       :version        version
       :payload-valid? payload-valid?
       :payload-bytes  (some-> payload count long)})
    {:present?       false
     :version        nil
     :payload-valid? false
     :payload-bytes  nil}))

(defn- ensure-giants!
  [conn key-count payload-bytes]
  (let [present (set (d/q '[:find [?key ...]
                            :where
                            [?e :giant/key ?key]]
                          @conn))
        missing (->> (range (long key-count))
                     (remove present)
                     (mapv #(giant-entity % initial-value payload-bytes)))]
    (when (seq missing)
      (d/transact! conn missing))))

(defn- local-node-giant-state
  [cluster-id logical-node key-count payload-bytes]
  (let [rows (local/local-query cluster-id
                                logical-node
                                giant-rows-query)]
    (if (= ::local/unavailable rows)
      {:values ::local/unavailable
       :node-diagnostics (local/node-diagnostics cluster-id logical-node)
       :ready? false}
      (let [values-by-key (into {}
                                (map (fn [[k version payload]]
                                       [(long k)
                                        {:version        (long version)
                                         :payload-valid? (= payload
                                                            (payload-for
                                                              k
                                                              version
                                                              payload-bytes))
                                         :payload-bytes  (count payload)}]))
                                rows)
            values       (mapv values-by-key (range (long key-count)))]
        {:values values
         :node-diagnostics (local/node-diagnostics cluster-id logical-node)
         :ready? (and (= (long key-count) (count values))
                      (every? (fn [{:keys [version payload-valid?]
                                     :as   row}]
                                (and (= (long initial-value) version)
                                     payload-valid?
                                     (= (long payload-bytes)
                                        (long (or (:payload-bytes row) 0)))))
                              values))}))))

(defn- wait-for-giants-visible-on-live-nodes!
  [cluster-id key-count payload-bytes]
  (let [timeout-ms (local/workload-setup-timeout-ms cluster-id
                                                    default-setup-timeout-ms)
        deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop [last-snapshot nil]
      (let [live-nodes (-> (local/cluster-state cluster-id) :live-nodes sort)
            snapshot   (into {}
                             (map (fn [logical-node]
                                    [logical-node
                                     (local-node-giant-state
                                       cluster-id
                                       logical-node
                                       key-count
                                       payload-bytes)]))
                             live-nodes)]
        (cond
          (every? (fn [[_ {:keys [ready?]}]]
                    ready?)
                  snapshot)
          snapshot

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info "Timed out waiting for giant values on live nodes"
                          {:cluster-id cluster-id
                           :timeout-ms timeout-ms
                           :payload-bytes payload-bytes
                           :snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn- ensure-giants-initialized!
  [test key-count payload-bytes]
  (let [cluster-id (:datalevin/cluster-id test)]
    (when-not (contains? @initialized-clusters cluster-id)
      (locking initialized-clusters
        (when-not (contains? @initialized-clusters cluster-id)
          (local/with-leader-conn
            test
            schema
            (fn [conn]
              (ensure-giants! conn key-count payload-bytes)))
          (wait-for-giants-visible-on-live-nodes!
            cluster-id
            key-count
            payload-bytes)
          (swap! initialized-clusters conj cluster-id))))))

(defn- keyed-value
  [op]
  (let [v (:value op)]
    [(long (key v)) (val v)]))

(defn- write-giant!
  [conn payload-bytes k version]
  (let [k       (long k)
        version (long version)]
    (if-some [[entid _ _] (first (d/q giant-state-query @conn k))]
      (d/transact! conn [{:db/id entid
                          :giant/key k
                          :giant/version version
                          :giant/payload (payload-for k version payload-bytes)}])
      (d/transact! conn [(giant-entity k version payload-bytes)]))
    ;; Linearizability should reflect the requested write value, not a later
    ;; overlapping state observed by an immediate readback.
    {:version        version
     :payload-valid? true
     :payload-bytes  (long payload-bytes)}))

(defn- cas-giant!
  [conn payload-bytes k [expected new-value]]
  (let [expected  (long expected)
        new-value (long new-value)
        payload   (payload-for k new-value payload-bytes)
        db        @conn
        current-row (first (d/q giant-state-query db (long k)))]
    (if-some [[entid current-version _] current-row]
      (if (= (long current-version) expected)
        (do
          (d/transact! conn [[:db/cas entid
                              :giant/version
                              expected
                              new-value]
                             [:db/add
                              entid
                              :giant/payload
                              payload]])
          {:version        new-value
           :payload-valid? true
           :payload-bytes  (long payload-bytes)})
        ::cas-failed)
      ::cas-failed)))

(defn- execute-op!
  [conn payload-bytes op]
  (let [[k v] (keyed-value op)]
    (case (:f op)
      :write
      (let [state (write-giant! conn payload-bytes k v)]
        {:tuple          (independent/tuple k (:version state))
         :payload-valid? (:payload-valid? state)
         :payload-bytes  (:payload-bytes state)})

      :read
      (let [state (giant-state @conn payload-bytes k)]
        {:tuple          (independent/tuple k (:version state))
         :payload-valid? (:payload-valid? state)
         :payload-bytes  (:payload-bytes state)})

      :cas
      (let [state (cas-giant! conn payload-bytes k v)]
        (if (= ::cas-failed state)
          ::cas-failed
          {:tuple          (independent/tuple k [(first v) (:version state)])
           :payload-valid? (:payload-valid? state)
           :payload-bytes  (:payload-bytes state)}))

      ::unsupported)))

(defn- op-error
  [e]
  (if (= :transact/cas (:error (ex-data e)))
    :cas-failed
    (or (ex-message e)
        (.getName (class e)))))

(defn- payload-checker
  [payload-bytes]
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [samples (->> history
                         (filter (fn [{:keys [type f]}]
                                   (and (= :ok type)
                                        (contains? #{:read :write :cas} f))))
                         (keep (fn [{:keys [f value] :as op}]
                                 (when-not (true? (:giant/payload-valid? op))
                                   {:f             f
                                    :value         value
                                    :payload-bytes (:giant/payload-bytes op)})))
                         vec)]
        {:valid?            (empty? samples)
         :invalid-count     (count samples)
         :expected-bytes    (long payload-bytes)
         :invalid-samples   (vec (take 10 samples))}))))

(defn- giant-checker
  [payload-bytes]
  (checker/compose
    {:linearizable
     (independent/checker
       (checker/compose
         {:linearizable (checker/linearizable
                          {:model (model/cas-register initial-value)})
          :timeline     (timeline/html)}))
     :payloads
     (payload-checker payload-bytes)}))

(defn- giant-generator
  [key-count worker-count per-key-limit]
  (independent/->ConcurrentGenerator
    worker-count
    (fn [_k]
      (->> (if (= 1 worker-count)
             (gen/mix [read-op write-op cas-op cas-op])
             (gen/reserve 1
                          read-op
                          (gen/mix [write-op cas-op cas-op])))
           (gen/limit per-key-limit)
           (gen/process-limit worker-count)))
    nil
    nil
    nil
    (range (long key-count))
    nil))

(defrecord Client [node key-count payload-bytes]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [this test]
    (ensure-giants-initialized! test key-count payload-bytes)
    this)

  (invoke! [this test op]
    (try
      (ensure-giants-initialized! test key-count payload-bytes)
      (local/with-leader-conn
        test
        schema
        (fn [conn]
          (let [result (execute-op! conn payload-bytes op)]
            (cond
              (= ::cas-failed result)
              (assoc op
                     :type :fail
                     :error :cas-failed)

              (= ::unsupported result)
              (assoc op
                     :type :fail
                     :error [:unsupported-client-op (:f op)])

              :else
              (assoc op
                     :type :ok
                     :value (:tuple result)
                     :giant/payload-valid? (:payload-valid? result)
                     :giant/payload-bytes (:payload-bytes result))))))
      (catch Throwable e
        (workload.util/assoc-exception-op op e (op-error e)))))

  (teardown! [this _test]
    this)

  (close! [_this _test]
    nil))

(defn workload
  [opts]
  (let [key-count     (long (:key-count opts 8))
        payload-bytes (long (or (:giant-payload-bytes opts)
                                default-payload-bytes))
        worker-count  (long (or (:concurrency opts)
                                (count (or (seq (:nodes opts))
                                           local/default-nodes))))
        per-key-limit (long (or (:max-writes-per-key opts) 32))]
    {:client    (->Client nil key-count payload-bytes)
     :generator (giant-generator key-count worker-count per-key-limit)
     :checker   (giant-checker payload-bytes)
     :schema    schema}))
