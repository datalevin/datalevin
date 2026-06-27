(ns datalevin.jepsen.workload.fencing
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [datalevin.client :as cl]
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [jepsen.checker :as checker]
   [jepsen.client :as client])
  (:import
   [java.util UUID]))

(def schema
  {:fencing/id    {:db/valueType :db.type/string
                   :db/unique :db.unique/identity}
   :fencing/probe {:db/valueType :db.type/string}
   :fencing/node  {:db/valueType :db.type/string}
   :fencing/value {:db/valueType :db.type/long}})

(def ^:private node-client-timeout-ms 5000)
(def ^:private node-probe-timeout-ms 2000)
(def ^:private node-write-timeout-ms 5000)
(def ^:private leader-wait-timeout-ms 1000)
(def ^:private sample-limit 5)
(def ^:private survivor-query
  '[:find ?id ?node ?value
    :in $ ?probe
    :where
    [?e :fencing/probe ?probe]
    [?e :fencing/id ?id]
    [?e :fencing/node ?node]
    [?e :fencing/value ?value]])

(def ^:private expected-write-failure-markers
  ["HA write admission rejected"
   "Timed out waiting for single leader"
   "Timeout in making request"
   "Unable to connect to server:"
   "Connection refused"])

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

(defn- ^:redef probe-node*
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

(defn- write-id
  [probe-id logical-node]
  (str probe-id "/" logical-node))

(defn- write-value
  [probe-id logical-node]
  (long (hash (write-id probe-id logical-node))))

(defn- write-datum
  [probe-id logical-node]
  {:db/id (str "fencing-" (write-id probe-id logical-node))
   :fencing/id (write-id probe-id logical-node)
   :fencing/probe probe-id
   :fencing/node logical-node
   :fencing/value (write-value probe-id logical-node)})

(defn- expected-write-failure-data?
  [message err-data transport?]
  (let [message (or message
                    (some-> (:error err-data) name))]
    (or transport?
        (= :ha/write-rejected (:error err-data))
        (and (string? message)
             (some #(str/includes? message %)
                   expected-write-failure-markers)))))

(defn- write-error-response
  [message err-data]
  (cond-> {:status (if (expected-write-failure-data? message err-data false)
                     :rejected
                     :error)
           :message message}
    (map? err-data)
    (assoc :err-data (select-keys err-data
                                  [:error
                                   :reason
                                   :retryable?
                                   :ha-authoritative-leader-endpoint
                                   :ha-authoritative-leader-node-id]))))

(defn- write-exception-result
  [e]
  (let [err-data (or (:err-data (ex-data e))
                     (ex-data e))
        message  (or (ex-message e)
                     (.getName (class e)))]
    (cond-> {:status (if (expected-write-failure-data?
                           message
                           err-data
                           (local/transport-failure? e))
                       (if (local/transport-failure? e)
                         :unreachable
                         :rejected)
                       :error)
             :message message
             :class (.getName (class e))}
      (map? err-data)
      (assoc :err-data (select-keys err-data
                                    [:error
                                     :reason
                                     :retryable?
                                     :ha-authoritative-leader-endpoint
                                     :ha-authoritative-leader-node-id])))))

(defn- unexpected-response
  [type]
  {:status :error
   :message (str "Unexpected response type " type)})

(defn- write-node*
  [test probe-id logical-node]
  (let [cluster-id (:datalevin/cluster-id test)
        db-name    (:db-name test)
        client*    (volatile! nil)]
    (try
      (let [client (node-client cluster-id logical-node)]
        (vreset! client* client)
        (let [{:keys [type message err-data]}
              (cl/request client {:type :open-transact
                                  :args [db-name]
                                  :writing? false})]
          (case type
            :command-complete
            (let [closed? (volatile! false)]
              (try
                (let [{:keys [type message err-data result]}
                      (cl/request client {:type :tx-data+db-info
                                          :mode :request
                                          :writing? true
                                          :args [db-name
                                                 [(write-datum probe-id
                                                               logical-node)]
                                                 false]})]
                  (case type
                    :command-complete
                    (let [{close-type     :type
                           close-message  :message
                           close-err-data :err-data}
                          (cl/request client {:type :close-transact
                                              :args [db-name]
                                              :writing? true})]
                      (case close-type
                        :command-complete
                        (do
                          (vreset! closed? true)
                          {:status :written
                           :tx (get-in result [:db-info :max-tx])
                           :tx-data-count (count (:tx-data result))})

                        :error-response
                        (write-error-response close-message close-err-data)

                        (unexpected-response close-type)))

                    :error-response
                    (write-error-response message err-data)

                    (unexpected-response type)))
                (finally
                  (when-not @closed?
                    (abort-open-transact! client db-name)))))

            :error-response
            (write-error-response message err-data)

            (unexpected-response type))))
      (catch Throwable e
        (write-exception-result e))
      (finally
        (when-let [client @client*]
          (try
            (cl/disconnect client)
            (catch Throwable _ nil)))))))

(defn- write-node!
  [test probe-id logical-node]
  (let [result-f (future
                   (write-node* test probe-id logical-node))
        result   (deref result-f node-write-timeout-ms ::timeout)]
    (if (= ::timeout result)
      (do
        (future-cancel result-f)
        {:status :unreachable
         :message "Timeout in making request"
         :timeout-ms node-write-timeout-ms})
      result)))

(defn- write-snapshot
  [test probe-id nodes]
  (let [futures (into {}
                      (map (fn [logical-node]
                             [logical-node
                              (future
                                (write-node! test probe-id logical-node))]))
                      nodes)]
    (into {}
          (map (fn [[logical-node result-f]]
                 [logical-node @result-f]))
          futures)))

(defn- survivor-snapshot
  [test probe-id]
  (try
    (local/with-leader-conn
      test
      schema
      (fn [conn]
        (let [rows (->> (d/q survivor-query @conn probe-id)
                        (mapv (fn [[id logical-node value]]
                                {:id id
                                 :node logical-node
                                 :value (long value)})))
              nodes (->> rows
                         (map :node)
                         distinct
                         sort
                         vec)]
          {:status :ok
           :rows rows
           :nodes nodes})))
    (catch Throwable e
      {:status :unavailable
       :message (or (ex-message e)
                    (.getName (class e)))
       :class (.getName (class e))})))

(defn- probe-snapshot
  [test]
  (let [cluster-id  (:datalevin/cluster-id test)
        nodes       (->> (:nodes test) sort vec)
        probe-id    (str (UUID/randomUUID))
        writes      (write-snapshot test probe-id nodes)
        survivors   (survivor-snapshot test probe-id)
        authoritative (local/maybe-wait-for-single-leader
                        cluster-id
                        leader-wait-timeout-ms)]
    {:probe-id probe-id
     :authoritative-leader (:leader authoritative)
     :writes writes
     :survivors survivors
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

(defn- write-result-nodes
  [snapshot status]
  (->> (:writes snapshot)
       (keep (fn [[logical-node result]]
               (when (= status (:status result))
                 logical-node)))
       sort
       vec))

(defn- write-errors
  [snapshot]
  (->> (:writes snapshot)
       (keep (fn [[logical-node {:keys [status] :as result}]]
               (when (= :error status)
                 (assoc result :node logical-node))))
       vec))

(defn- survivor-nodes
  [snapshot]
  (if (= :ok (get-in snapshot [:survivors :status]))
    (-> snapshot :survivors :nodes vec)
    []))

(defn- lost-acknowledged-writes
  [snapshot]
  (when (= :ok (get-in snapshot [:survivors :status]))
    (let [written   (set (write-result-nodes snapshot :written))
          survivors (set (survivor-nodes snapshot))]
      (->> (set/difference written survivors)
           sort
           vec))))

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
                                     vec)
            write-snapshots     (mapv (fn [snapshot]
                                        {:snapshot snapshot
                                         :written (write-result-nodes snapshot
                                                                     :written)
                                         :survivors (survivor-nodes snapshot)})
                                      probes)
            write-split-brain   (->> write-snapshots
                                     (filter (fn [{:keys [written]}]
                                               (> (count written) 1)))
                                     vec)
            survivor-split-brain (->> write-snapshots
                                      (filter (fn [{:keys [survivors]}]
                                                (> (count survivors) 1)))
                                      vec)
            lost-writes         (->> probes
                                     (keep (fn [snapshot]
                                             (when-let [lost
                                                        (seq
                                                          (lost-acknowledged-writes
                                                            snapshot))]
                                               {:probe-id (:probe-id snapshot)
                                                :lost (vec lost)
                                                :written (write-result-nodes
                                                           snapshot
                                                           :written)
                                                :survivors (survivor-nodes
                                                             snapshot)})))
                                     vec)
            unexpected-write-errors (->> probes
                                         (mapcat write-errors)
                                         vec)
            single-writer-count (count (filter (fn [{:keys [written]}]
                                                 (= 1 (count written)))
                                               write-snapshots))
            single-survivor-count (count (filter (fn [{:keys [survivors]}]
                                                   (= 1 (count survivors)))
                                                 write-snapshots))
            write-survivor-verified-count
            (count (filter (fn [{:keys [written survivors]}]
                             (and (= 1 (count written))
                                  (= written survivors)))
                           write-snapshots))
            failed? (or (seq split-brain)
                        (seq unexpected-errors)
                        (seq leader-mismatches)
                        (seq write-split-brain)
                        (seq survivor-split-brain)
                        (seq lost-writes)
                        (seq unexpected-write-errors))
            valid? (cond
                     failed? false
                     (pos? write-survivor-verified-count) true
                     :else :unknown)]
        {:valid? valid?
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
         :unexpected-error-samples (take-sample unexpected-errors)
         :single-writer-count single-writer-count
         :single-survivor-count single-survivor-count
         :write-survivor-verified-count write-survivor-verified-count
         :write-split-brain-count (count write-split-brain)
         :write-split-brain-samples
         (take-sample
           (map (fn [{:keys [snapshot written survivors]}]
                  {:probe-id (:probe-id snapshot)
                   :written written
                   :survivors survivors
                   :writes (:writes snapshot)
                   :survivor-result (:survivors snapshot)})
                write-split-brain))
         :survivor-split-brain-count (count survivor-split-brain)
         :survivor-split-brain-samples
         (take-sample
           (map (fn [{:keys [snapshot written survivors]}]
                  {:probe-id (:probe-id snapshot)
                   :written written
                   :survivors survivors
                   :survivor-result (:survivors snapshot)})
                survivor-split-brain))
         :lost-acknowledged-write-count (count lost-writes)
         :lost-acknowledged-write-samples (take-sample lost-writes)
         :unexpected-write-error-count (count unexpected-write-errors)
         :unexpected-write-error-samples
         (take-sample unexpected-write-errors)}))))

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
