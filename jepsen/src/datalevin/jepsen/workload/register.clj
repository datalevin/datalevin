(ns datalevin.jepsen.workload.register
  (:require
   [datalevin.core :as d]
   [datalevin.jepsen.init-cache :as init-cache]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.workload.util :as workload.util]
   [jepsen.checker :as checker]
   [jepsen.checker.timeline :as timeline]
   [jepsen.client :as client]
   [jepsen.generator :as gen]
   [jepsen.independent :as independent]
   [knossos.model :as model]))

(def schema
  {:register/key   {:db/valueType :db.type/long
                    :db/unique :db.unique/identity}
   :register/value {:db/valueType :db.type/long}})

(def ^:private initial-value 0)
(def ^:private default-setup-timeout-ms 15000)
(defonce ^:private initialized-clusters (init-cache/cluster-cache))
(def ^:private register-rows-query
  '[:find ?key ?value
    :where
    [?e :register/key ?key]
    [?e :register/value ?value]])
(def ^:private register-state-query
  '[:find ?e ?value
    :in $ ?key
    :where
    [?e :register/key ?key]
    [?e :register/value ?value]])

(defn- write-op
  [_ _]
  {:type :invoke
   :f :write
   :value (rand-int 5)})

(defn- read-op
  [_ _]
  {:type :invoke
   :f :read})

(defn- cas-op
  [_ _]
  {:type :invoke
   :f :cas
   :value [(rand-int 5) (rand-int 5)]})

(defn- register-value
  [db k]
  (some->> (d/q register-state-query db (long k))
           first
           second
           long))

(defn- ensure-registers!
  [conn key-count]
  (let [present (set (d/q '[:find [?key ...]
                            :where
                            [?e :register/key ?key]]
                          @conn))
        missing (->> (range (long key-count))
                     (remove present)
                     (mapv (fn [k]
                             {:db/id (str "register-" k)
                              :register/key (long k)
                              :register/value (long initial-value)})))]
    (when (seq missing)
      (d/transact! conn missing))))

(defn- local-node-register-state
  [cluster-id logical-node key-count]
  (let [rows (local/local-query cluster-id
                                logical-node
                                register-rows-query)]
    (if (= ::local/unavailable rows)
      {:values ::local/unavailable
       :node-diagnostics (local/node-diagnostics cluster-id logical-node)
       :ready? false}
      (let [values-by-key (into {}
                                (map (fn [[k v]]
                                       [(long k) (long v)]))
                                rows)
            values       (mapv values-by-key (range (long key-count)))]
        {:values values
         :node-diagnostics (local/node-diagnostics cluster-id logical-node)
         :ready? (and (= (long key-count) (count values))
                      (every? integer? values))}))))

(defn- wait-for-registers-visible-on-live-nodes!
  [cluster-id key-count]
  (let [timeout-ms (local/workload-setup-timeout-ms cluster-id
                                                    default-setup-timeout-ms)
        deadline (+ (System/currentTimeMillis) timeout-ms)
        expected (vec (repeat (long key-count) (long initial-value)))]
    (loop [last-snapshot nil]
      (let [live-nodes (-> (local/cluster-state cluster-id) :live-nodes sort)
            snapshot   (into {}
                             (map (fn [logical-node]
                                    [logical-node
                                     (local-node-register-state
                                      cluster-id
                                      logical-node
                                      key-count)]))
                             live-nodes)]
        (cond
          (every? (fn [[_ {:keys [ready? values]}]]
                    (and ready?
                         (= expected values)))
                  snapshot)
          snapshot

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info "Timed out waiting for register seed state on live nodes"
                          {:cluster-id cluster-id
                           :timeout-ms timeout-ms
                           :expected-values expected
                           :snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn- ensure-registers-initialized!
  [test key-count]
  (let [cluster-id (:datalevin/cluster-id test)]
    (when-not (contains? @initialized-clusters cluster-id)
      (locking initialized-clusters
        (when-not (contains? @initialized-clusters cluster-id)
          (workload.util/with-retrying-leader-conn
            test
            schema
            (local/workload-setup-timeout-ms cluster-id
                                             default-setup-timeout-ms)
            (fn [conn]
              (ensure-registers! conn key-count)))
          (wait-for-registers-visible-on-live-nodes! cluster-id key-count)
          (swap! initialized-clusters conj cluster-id))))))

(defn- keyed-value
  [op]
  (let [v (:value op)]
    [(long (key v)) (val v)]))

(defn- write-register!
  [conn k v]
  (let [k (long k)
        v (long v)]
    (if-some [[entid _] (first (d/q register-state-query @conn k))]
      (d/transact! conn [{:db/id entid
                          :register/key k
                          :register/value v}])
      (d/transact! conn [{:register/key k
                          :register/value v}])))
  (long v))

(defn- cas-register!
  [conn k [expected new-value]]
  (let [expected (long expected)
        new-value (long new-value)
        k         (long k)]
    (if-some [[entid _] (first (d/q register-state-query @conn k))]
      (try
        (d/transact! conn [[:db/cas entid
                            :register/value
                            expected
                            new-value]])
        [expected new-value]
        (catch Throwable e
          (if (or (= :transact/cas (:error (ex-data e)))
                  (when-some [message (ex-message e)]
                    (re-find #":db\.fn/cas failed" message)))
            ::cas-failed
            (throw e))))
      ::cas-failed)))

(defn- execute-op!
  [conn op]
  (let [[k v] (keyed-value op)]
    (case (:f op)
      :write
      (independent/tuple k (write-register! conn k v))

      :read
      (independent/tuple k (register-value @conn k))

      :cas
      (let [result (cas-register! conn k v)]
        (if (= ::cas-failed result)
          ::cas-failed
          (independent/tuple k result)))

      ::unsupported)))

(defn- op-error
  [e]
  (if (or (= :transact/cas (:error (ex-data e)))
          (when-some [message (ex-message e)]
            (re-find #":db\.fn/cas failed" message)))
    :cas-failed
    (or (ex-message e)
        (.getName (class e)))))

(defn- follower-read-node
  [test]
  (let [cluster-id  (:datalevin/cluster-id test)
        leader      (:leader (local/wait-for-single-leader! cluster-id))
        live-nodes  (-> (local/cluster-state cluster-id) :live-nodes sort vec)
        followers   (->> live-nodes
                         (remove #{leader})
                         (filter (fn [logical-node]
                                   (= :follower
                                      (:ha-role
                                       (local/node-diagnostics cluster-id
                                                               logical-node)))))
                         vec)]
    (or (when (seq followers)
          (rand-nth followers))
        leader)))

(defn- complete-op
  [op result]
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
           :value result)))

(defn- execute-follower-read-op!
  [test op]
  (let [read-node (follower-read-node test)]
    (local/with-node-conn
      test
      read-node
      schema
      (fn [conn]
        (assoc (complete-op op (execute-op! conn op))
               :datalevin/read-node read-node)))))

(defn- execute-leader-op!
  [test op]
  (local/with-leader-conn
    test
    schema
    (fn [conn]
      (complete-op op (execute-op! conn op)))))

(defn- register-checker
  []
  (independent/checker
    (checker/compose
      {:linearizable (checker/linearizable
                       {:model (model/cas-register initial-value)})
       :timeline (timeline/html)})))

(defn- register-generator
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

(defn- final-read-generator
  [key-count]
  (independent/sequential-generator
    (range (long key-count))
    (fn [_k]
      [{:type :invoke
        :f :read}])))

(defrecord Client [node key-count]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [this test]
    (ensure-registers-initialized! test key-count)
    this)

  (invoke! [this test op]
    (try
      (ensure-registers-initialized! test key-count)
      (execute-leader-op! test op)
      (catch Throwable e
        (workload.util/assoc-exception-op op e (op-error e)))))

  (teardown! [this _test]
    this)

  (close! [_this _test]
    nil))

(defrecord FollowerReadClient [node key-count]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [this test]
    (ensure-registers-initialized! test key-count)
    this)

  (invoke! [this test op]
    (try
      (ensure-registers-initialized! test key-count)
      (if (= :read (:f op))
        (execute-follower-read-op! test op)
        (execute-leader-op! test op))
      (catch Throwable e
        (workload.util/assoc-exception-op op e (op-error e)))))

  (teardown! [this _test]
    this)

  (close! [_this _test]
    nil))

(defn- build-workload
  [opts client-factory]
  (let [key-count (long (:key-count opts 8))
        worker-count (long (or (:concurrency opts)
                               (count (or (seq (:nodes opts))
                                          local/default-nodes))))
        per-key-limit (long (or (:max-writes-per-key opts) 32))]
    {:client (client-factory key-count)
     :generator (register-generator key-count worker-count per-key-limit)
     :final-generator (final-read-generator key-count)
     :checker (register-checker)
     :schema schema}))

(defn workload
  [opts]
  (build-workload opts #(->Client nil %)))

(defn follower-read-workload
  [opts]
  (build-workload opts #(->FollowerReadClient nil %)))
