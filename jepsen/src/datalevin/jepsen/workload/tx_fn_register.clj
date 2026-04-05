(ns datalevin.jepsen.workload.tx-fn-register
  (:require
   [datalevin.core :as d]
   [datalevin.interpret :as i]
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
  {:txreg/key     {:db/valueType :db.type/long
                   :db/unique :db.unique/identity}
   :txreg/version {:db/valueType :db.type/long}
   :txreg/payload {:db/valueType :db.type/string}})

(def ^:private initial-value 0)
(def ^:private default-payload-bytes 12000)
(def ^:private tx-fn-padding
  (apply str (repeat 512 "tx-fn-register-padding-")))
(def ^:private default-setup-timeout-ms 15000)
(defonce ^:private initialized-clusters (init-cache/cluster-cache))
(def ^:private txreg-rows-query
  '[:find ?key ?version ?payload
    :where
    [?e :txreg/key ?key]
    [?e :txreg/version ?version]
    [?e :txreg/payload ?payload]])
(def ^:private txreg-state-query
  '[:find ?e ?version ?payload
    :in $ ?key
    :where
    [?e :txreg/key ?key]
    [?e :txreg/version ?version]
    [?e :txreg/payload ?payload]])
(def ^:private tx-fn-query
  '[:find ?ident ?fn
    :in $ [?ident ...]
    :where
    [?e :db/ident ?ident]
    [?e :db/fn ?fn]])

(defn- payload-for
  [k version payload-bytes]
  (let [payload-bytes (long payload-bytes)
        prefix        (str "txreg|" (long k) "|" (long version) "|")
        filler        (str "payload-" (long k) "-" (long version) "-")
        sb            (StringBuilder. prefix)]
    (when (< payload-bytes (count prefix))
      (throw (ex-info "tx-fn payload size is smaller than payload prefix"
                      {:payload-bytes payload-bytes
                       :prefix-bytes  (count prefix)})))
    (while (< (.length sb) payload-bytes)
      (.append sb filler))
    (.substring (.toString sb) 0 (int payload-bytes))))

(defn- txreg-entity
  [k version payload-bytes]
  {:db/id         (str "txreg-" (long k))
   :txreg/key     (long k)
   :txreg/version (long version)
   :txreg/payload (payload-for k version payload-bytes)})

(defn- make-txreg-write
  []
  (let [padding      tx-fn-padding
        entid-query '[:find ?e .
                      :in $ ?k
                      :where
                      [?e :txreg/key ?k]]]
    (i/inter-fn [db k version payload]
      (let [_       padding
            k       (long k)
            version (long version)
            entid   (d/q entid-query db k)]
        (if (some? entid)
          [{:db/id         entid
            :txreg/version version
            :txreg/payload payload}]
          [{:db/id         (str "txreg-" k)
            :txreg/key     k
            :txreg/version version
            :txreg/payload payload}])))))

(defn- make-txreg-cas
  []
  (let [padding      tx-fn-padding
        version-query '[:find ?e ?version
                        :in $ ?k
                        :where
                        [?e :txreg/key ?k]
                        [?e :txreg/version ?version]]]
    (i/inter-fn [db k expected new-value payload]
      (let [_         padding
            k         (long k)
            expected  (long expected)
            new-value (long new-value)]
        (if-some [[entid current-version] (first (d/q version-query db k))]
          (if (= expected (long current-version))
            [{:db/id         entid
              :txreg/version new-value
              :txreg/payload payload}]
            [])
          [])))))

(def ^:private txreg-write (make-txreg-write))

(def ^:private txreg-cas (make-txreg-cas))

(def ^:private tx-fn-entities
  [{:db/ident :txreg/write
    :db/fn    txreg-write}
   {:db/ident :txreg/cas
    :db/fn    txreg-cas}])

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

(defn- txreg-state
  [db payload-bytes k]
  (if-some [[_ version payload] (first (d/q txreg-state-query db (long k)))]
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

(defn- ensure-tx-fns!
  [conn]
  (let [db      @conn
        idents  (mapv :db/ident tx-fn-entities)
        present (set (d/q '[:find [?ident ...]
                            :in $ [?ident ...]
                            :where
                            [?e :db/ident ?ident]
                            [?e :db/fn ?fn]]
                          db
                          idents))
        missing (->> tx-fn-entities
                     (remove (comp present :db/ident))
                     vec)]
    (when (seq missing)
      (d/transact! conn missing))))

(defn- ensure-registers!
  [conn key-count payload-bytes]
  (let [present (set (d/q '[:find [?key ...]
                            :where
                            [?e :txreg/key ?key]]
                          @conn))
        missing (->> (range (long key-count))
                     (remove present)
                     (mapv #(txreg-entity % initial-value payload-bytes)))]
    (when (seq missing)
      (d/transact! conn missing))))

(defn- local-node-txreg-state
  [cluster-id logical-node key-count payload-bytes]
  (let [rows       (local/local-query cluster-id
                                      logical-node
                                      txreg-rows-query)
        tx-fn-rows (local/local-query cluster-id
                                      logical-node
                                      tx-fn-query
                                      (mapv :db/ident tx-fn-entities))]
    (cond
      (= ::local/unavailable rows)
      {:values ::local/unavailable
       :tx-fns-visible? false
       :node-diagnostics (local/node-diagnostics cluster-id logical-node)
       :ready? false}

      (= ::local/unavailable tx-fn-rows)
      {:values rows
       :tx-fns-visible? false
       :node-diagnostics (local/node-diagnostics cluster-id logical-node)
       :ready? false}

      :else
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
            values       (mapv values-by-key (range (long key-count)))
            tx-fns       (into {}
                             (map (fn [[ident fun]]
                                    [ident (fn? fun)]))
                             tx-fn-rows)]
        {:values values
         :tx-fns-visible? (every? true? (vals tx-fns))
         :node-diagnostics (local/node-diagnostics cluster-id logical-node)
         :ready? (and (= (count tx-fn-entities) (count tx-fns))
                      (every? true? (vals tx-fns))
                      (= (long key-count) (count values))
                      (every? (fn [{:keys [version payload-valid?]
                                     :as   row}]
                                (and (= (long initial-value) version)
                                     payload-valid?
                                     (= (long payload-bytes)
                                        (long (or (:payload-bytes row) 0)))))
                              values))}))))

(defn- wait-for-txreg-visible-on-live-nodes!
  [cluster-id key-count payload-bytes]
  (let [timeout-ms (local/workload-setup-timeout-ms cluster-id
                                                    default-setup-timeout-ms)
        deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop [last-snapshot nil]
      (let [live-nodes (-> (local/cluster-state cluster-id) :live-nodes sort)
            snapshot   (into {}
                             (map (fn [logical-node]
                                    [logical-node
                                     (local-node-txreg-state
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
          (throw (ex-info "Timed out waiting for tx-fn register state"
                          {:cluster-id cluster-id
                           :timeout-ms timeout-ms
                           :payload-bytes payload-bytes
                           :snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn- ensure-txreg-initialized!
  [test key-count payload-bytes]
  (let [cluster-id (:datalevin/cluster-id test)]
    (when-not (contains? @initialized-clusters cluster-id)
      (locking initialized-clusters
        (when-not (contains? @initialized-clusters cluster-id)
          (local/with-leader-conn
            test
            schema
            (fn [conn]
              (ensure-tx-fns! conn)
              (ensure-registers! conn key-count payload-bytes)))
          (wait-for-txreg-visible-on-live-nodes!
            cluster-id
            key-count
            payload-bytes)
          (swap! initialized-clusters conj cluster-id))))))

(defn- keyed-value
  [op]
  (let [v (:value op)]
    [(long (key v)) (val v)]))

(defn- write-via-tx-fn!
  [conn payload-bytes k version]
  (let [version (long version)
        payload (payload-for k version payload-bytes)]
    (d/transact! conn [[:txreg/write (long k) version payload]])
    (txreg-state @conn payload-bytes k)))

(defn- cas-via-tx-fn!
  [conn payload-bytes k [expected new-value]]
  (let [expected  (long expected)
        new-value (long new-value)
        payload   (payload-for k new-value payload-bytes)
        report    (d/transact! conn [[:txreg/cas
                                      (long k)
                                      expected
                                      new-value
                                      payload]])]
    (if (seq (:tx-data report))
      (txreg-state @conn payload-bytes k)
      ::cas-failed)))

(defn- execute-op!
  [conn payload-bytes op]
  (let [[k v] (keyed-value op)]
    (case (:f op)
      :write
      (let [state (write-via-tx-fn! conn payload-bytes k v)]
        {:tuple          (independent/tuple k (:version state))
         :payload-valid? (:payload-valid? state)
         :payload-bytes  (:payload-bytes state)})

      :read
      (let [state (txreg-state @conn payload-bytes k)]
        {:tuple          (independent/tuple k (:version state))
         :payload-valid? (:payload-valid? state)
         :payload-bytes  (:payload-bytes state)})

      :cas
      (let [state (cas-via-tx-fn! conn payload-bytes k v)]
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
                                 (when-not (true? (:txreg/payload-valid? op))
                                   {:f             f
                                    :value         value
                                    :payload-bytes (:txreg/payload-bytes op)})))
                         vec)]
        {:valid?            (empty? samples)
         :invalid-count     (count samples)
         :expected-bytes    (long payload-bytes)
         :invalid-samples   (vec (take 10 samples))}))))

(defn- txreg-checker
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

(defn- txreg-generator
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
    (ensure-txreg-initialized! test key-count payload-bytes)
    this)

  (invoke! [this test op]
    (try
      (ensure-txreg-initialized! test key-count payload-bytes)
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
                     :txreg/payload-valid? (:payload-valid? result)
                     :txreg/payload-bytes (:payload-bytes result))))))
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
     :generator (txreg-generator key-count worker-count per-key-limit)
     :checker   (txreg-checker payload-bytes)
     :schema    schema}))
