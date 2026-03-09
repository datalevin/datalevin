(ns datalevin.jepsen.workload.register
  (:require
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
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
(def ^:private setup-timeout-ms 15000)
(defonce ^:private initialized-clusters (atom #{}))
(def ^:private register-rows-query
  '[:find ?key ?value
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
  (some-> (d/entity db [:register/key (long k)])
          :register/value
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
  (let [deadline (+ (System/currentTimeMillis) setup-timeout-ms)
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
                           :timeout-ms setup-timeout-ms
                           :expected-values expected
                           :snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn- ensure-registers-initialized!
  [test key-count]
  (let [cluster-id (:datalevin/cluster-id test)]
    (when-not (contains? @initialized-clusters cluster-id)
      (locking initialized-clusters
        (when-not (contains? @initialized-clusters cluster-id)
          (local/with-leader-conn
            test
            schema
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
  (d/transact! conn [{:register/key (long k)
                      :register/value (long v)}])
  (long v))

(defn- cas-register!
  [conn k [expected new-value]]
  (let [expected (long expected)
        new-value (long new-value)
        db @conn
        current (register-value db k)]
    (if (= current expected)
      (do
        (d/transact! conn [[:db/cas
                            [:register/key (long k)]
                            :register/value
                            expected
                            new-value]])
        [expected new-value])
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
  (if (= :transact/cas (:error (ex-data e)))
    :cas-failed
    (or (ex-message e)
        (.getName (class e)))))

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
      (local/with-leader-conn
        test
        schema
        (fn [conn]
          (let [result (execute-op! conn op)]
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
                     :value result)))))
      (catch Throwable e
        (assoc op
               :type :fail
               :error (op-error e)))))

  (teardown! [this _test]
    this)

  (close! [_this _test]
    nil))

(defn workload
  [opts]
  (let [key-count (long (:key-count opts 8))
        worker-count (long (or (:concurrency opts)
                               (count (or (seq (:nodes opts))
                                          local/default-nodes))))
        per-key-limit (long (or (:max-writes-per-key opts) 32))]
    {:client (->Client nil key-count)
     :generator (register-generator key-count worker-count per-key-limit)
     :checker (register-checker)
     :schema schema}))
