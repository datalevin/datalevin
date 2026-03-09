(ns datalevin.jepsen.workload.fencing
  (:require
   [datalevin.client :as cl]
   [datalevin.jepsen.local :as local]
   [jepsen.checker :as checker]
   [jepsen.client :as client]))

(def schema {})

(def ^:private node-client-timeout-ms 5000)
(def ^:private node-probe-timeout-ms 2000)
(def ^:private leader-wait-timeout-ms 1000)
(def ^:private sample-limit 5)

(defn- node-client
  [cluster-id logical-node]
  (cl/new-client (local/admin-uri
                   (local/endpoint-for-node cluster-id logical-node))
                 {:pool-size 1
                  :time-out node-client-timeout-ms}))

(defn- abort-open-transact!
  [client db-name]
  (try
    (cl/request client {:type :abort-transact
                        :args [db-name]
                        :writing? true})
    (catch Throwable _
      nil)))

(defn- probe-node*
  [cluster-id db-name logical-node]
  (let [client* (volatile! nil)]
    (try
      (let [client (node-client cluster-id logical-node)]
        (vreset! client* client)
        (let [{:keys [type message err-data]}
              (cl/request client {:type :open-transact
                                  :args [db-name]})]
          (cond
            (= type :command-complete)
            (do
              (abort-open-transact! client db-name)
              {:status :admitted})

            (= type :error-response)
            {:status :rejected
             :server-message message
             :error (:error err-data)
             :reason (:reason err-data)
             :retryable? (:retryable? err-data)
             :leader-endpoint (:ha-authoritative-leader-endpoint err-data)
             :retry-endpoints (:ha-retry-endpoints err-data)}

            :else
            {:status :error
             :message (str "Unexpected response type " type)})))
      (catch Throwable e
        (if (local/transport-failure? e)
          {:status :unreachable
           :message (or (ex-message e)
                        (.getName (class e)))}
          {:status :error
           :message (or (ex-message e)
                        (.getName (class e)))}))
      (finally
        (when-let [client @client*]
          (try
            (cl/disconnect client)
            (catch Throwable _ nil)))))))

(defn- probe-node!
  [cluster-id db-name logical-node]
  (let [result-f (future
                   (probe-node* cluster-id db-name logical-node))
        result   (deref result-f node-probe-timeout-ms ::timeout)]
    (if (= ::timeout result)
      (do
        (future-cancel result-f)
        {:status :unreachable
         :message "Timeout in making request"
         :timeout-ms node-probe-timeout-ms})
      result)))

(defn- probe-snapshot
  [test]
  (let [cluster-id  (:datalevin/cluster-id test)
        nodes       (->> (:nodes test) sort vec)
        authoritative (local/maybe-wait-for-single-leader
                        cluster-id
                        leader-wait-timeout-ms)]
    {:authoritative-leader (:leader authoritative)
     :nodes (into {}
                  (map (fn [logical-node]
                         [logical-node
                          (probe-node! cluster-id
                                       (:db-name test)
                                       logical-node)]))
                  nodes)}))

(defn- fencing-op
  []
  {:type :invoke
   :f :probe})

(defn- admitted-nodes
  [snapshot]
  (->> (get snapshot :nodes)
       (keep (fn [[logical-node {:keys [status]}]]
               (when (= :admitted status)
                 logical-node)))
       vec))

(defn- probe-errors
  [snapshot]
  (->> (get snapshot :nodes)
       (keep (fn [[logical-node {:keys [status] :as result}]]
               (when (= :error status)
                 (assoc result :node logical-node))))
       vec))

(defn- take-sample
  [xs]
  (vec (take sample-limit xs)))

(defn- fencing-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [probes              (->> history
                                     (filter (fn [{:keys [type f value]}]
                                               (and (= :ok type)
                                                    (= :probe f)
                                                    (map? value))))
                                     (map :value)
                                     vec)
            admitted-snapshots  (mapv (fn [snapshot]
                                        {:snapshot snapshot
                                         :admitted (admitted-nodes snapshot)})
                                      probes)
            split-brain         (->> admitted-snapshots
                                     (filter (fn [{:keys [admitted]}]
                                               (> (count admitted) 1)))
                                     vec)
            unexpected-errors   (->> probes
                                     (mapcat probe-errors)
                                     vec)
            single-leader-count (count (filter (fn [{:keys [admitted]}]
                                                 (= 1 (count admitted)))
                                               admitted-snapshots))
            zero-leader-count   (count (filter (fn [{:keys [admitted]}]
                                                 (zero? (count admitted)))
                                               admitted-snapshots))
            leader-mismatches   (->> admitted-snapshots
                                     (keep (fn [{:keys [snapshot admitted]}]
                                             (let [authoritative
                                                   (:authoritative-leader
                                                     snapshot)]
                                               (when (and (= 1 (count admitted))
                                                          authoritative
                                                          (not= authoritative
                                                                (first admitted)))
                                                 {:authoritative-leader
                                                  authoritative
                                                  :admitted (first admitted)
                                                  :snapshot snapshot}))))
                                     vec)]
        {:valid? (and (empty? split-brain)
                      (empty? unexpected-errors)
                      (empty? leader-mismatches)
                      (pos? single-leader-count))
         :probe-count (count probes)
         :single-leader-count single-leader-count
         :zero-leader-count zero-leader-count
         :split-brain-count (count split-brain)
         :split-brain-samples
         (take-sample
           (map (fn [{:keys [snapshot admitted]}]
                  {:admitted admitted
                   :snapshot snapshot})
                split-brain))
         :leader-mismatch-count (count leader-mismatches)
         :leader-mismatch-samples (take-sample leader-mismatches)
         :unexpected-error-count (count unexpected-errors)
         :unexpected-error-samples (take-sample unexpected-errors)}))))

(defrecord Client [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [this _test]
    this)

  (invoke! [this test op]
    (if (not= :probe (:f op))
      (assoc op
             :type :fail
             :error [:unsupported-client-op (:f op)])
      (assoc op
             :type :ok
             :value (probe-snapshot test))))

  (teardown! [this _test]
    this)

  (close! [_this _test]
    nil))

(defn workload
  [_opts]
  {:client (->Client nil)
   :generator (repeatedly fencing-op)
   :final-generator {:type :invoke :f :probe}
   :checker (fencing-checker)
   :schema schema})
