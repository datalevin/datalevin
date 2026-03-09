(ns datalevin.jepsen.workload.rejoin-bootstrap
  (:require
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [jepsen.checker :as checker]
   [jepsen.client :as client]))

(def schema
  {:register/key   {:db/valueType :db.type/long
                    :db/unique :db.unique/identity}
   :register/value {:db/valueType :db.type/long}})

(def ^:private initial-value 0)
(def ^:private setup-timeout-ms 15000)
(def ^:private converge-timeout-ms 30000)
(def ^:private wal-gap-gc-timeout-ms 10000)
(def ^:private wal-gap-retry-sleep-ms 250)
(def ^:private wal-gap-write-sleep-ms 150)
(def ^:private wal-gap-writes-per-batch 4)
(def ^:private wal-gap-segment-max-ms 100)
(def ^:private wal-gap-replica-floor-ttl-ms 500)
(def ^:private sample-limit 10)
(defonce ^:private initialized-clusters (atom #{}))
(defonce ^:private converged-clusters (atom {}))
(def ^:private cluster-opts
  {:wal-segment-max-ms wal-gap-segment-max-ms
   :wal-segment-prealloc? false
   :wal-segment-prealloc-mode :none
   :wal-replica-floor-ttl-ms wal-gap-replica-floor-ttl-ms
   :snapshot-scheduler? false})
(def ^:private register-rows-query
  '[:find ?key ?value
    :where
    [?e :register/key ?key]
    [?e :register/value ?value]])

(defn- write-op
  [key-count]
  {:type :invoke
   :f :write
   :value (clojure.lang.MapEntry. (long (rand-int (int key-count)))
                                  (long (rand-int 5)))})

(defn- read-op
  [key-count]
  {:type :invoke
   :f :read
   :value (clojure.lang.MapEntry. (long (rand-int (int key-count))) nil)})

(defn- cas-op
  [key-count]
  {:type :invoke
   :f :cas
   :value (clojure.lang.MapEntry. (long (rand-int (int key-count)))
                                  [(long (rand-int 5))
                                   (long (rand-int 5))])})

(defn- register-values-from-rows
  [rows key-count]
  (let [values-by-key (into {}
                            (map (fn [[k v]]
                                   [(long k) (long v)]))
                            rows)]
    (mapv (fn [k]
            (get values-by-key (long k)))
          (range (long key-count)))))

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
      (let [values (register-values-from-rows rows key-count)]
        {:values values
         :node-diagnostics (local/node-diagnostics cluster-id logical-node)
         :ready? (and (= (long key-count) (count values))
                      (every? integer? values))}))))

(defn- wait-for-expected-registers-on-live-nodes!
  [cluster-id key-count expected-values timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
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
                         (= expected-values values)))
                  snapshot)
          snapshot

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info "Timed out waiting for register convergence on live nodes"
                          {:cluster-id cluster-id
                           :timeout-ms timeout-ms
                           :expected-values expected-values
                           :snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn- wait-for-registers-visible-on-live-nodes!
  [cluster-id key-count]
  (let [expected (vec (repeat (long key-count) (long initial-value)))]
    (wait-for-expected-registers-on-live-nodes!
      cluster-id
      key-count
      expected
      setup-timeout-ms)))

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
  (clojure.lang.MapEntry. k (long v)))

(defn- cas-register!
  [conn k [expected new-value]]
  (let [expected  (long expected)
        new-value (long new-value)
        db        @conn
        current   (register-value db k)]
    (if (= current expected)
      (do
        (d/transact! conn [[:db/cas
                            [:register/key (long k)]
                            :register/value
                            expected
                            new-value]])
        (clojure.lang.MapEntry. k [expected new-value]))
      ::cas-failed)))

(defn- leader-register-values
  [test key-count]
  (local/with-leader-conn
    test
    schema
    (fn [conn]
      (register-values-from-rows
        (d/q register-rows-query @conn)
        key-count))))

(defn- write-register-batch-with-rolls!
  [test key-count start-value n sleep-ms]
  (local/with-leader-conn
    test
    schema
    (fn [conn]
      (ensure-registers! conn key-count)
      (doseq [offset (range (long n))]
        (write-register! conn
                         (long (mod offset (long key-count)))
                         (long (+ (long start-value) offset)))
        (Thread/sleep (long sleep-ms)))))
  (let [cluster-id (:datalevin/cluster-id test)
        {:keys [leader]} (local/wait-for-single-leader! cluster-id
                                                        converge-timeout-ms)
        target-lsn (local/effective-local-lsn cluster-id leader)]
    (local/wait-for-live-nodes-at-least-lsn! cluster-id
                                             target-lsn
                                             converge-timeout-ms)
    target-lsn))

(defn- merge-gc-result
  [old new]
  (let [old-min (long (or (get-in old [:after :min-retained-lsn]) 0))
        new-min (long (or (get-in new [:after :min-retained-lsn]) 0))
        old-del (long (or (:deleted-count old) 0))
        new-del (long (or (:deleted-count new) 0))
        best    (cond
                  (> new-min old-min) new
                  (> old-min new-min) old
                  (> new-del old-del) new
                  :else new)]
    (assoc best :deleted-count (max old-del new-del))))

(defn- wal-gap-realized?
  [follower-next-lsn gc-results]
  (every? (fn [[_ result]]
            (and (pos? (long (or (:deleted-count result) 0)))
                 (> (long (or (get-in result [:after :min-retained-lsn]) 0))
                    (long follower-next-lsn))))
          gc-results))

(defn- wait-for-real-wal-gap!
  [cluster-id source-nodes follower-next-lsn]
  (let [deadline (+ (System/currentTimeMillis) wal-gap-gc-timeout-ms)]
    (loop [best-results {}]
      (let [attempt-results (into {}
                                  (map (fn [logical-node]
                                         [logical-node
                                          (local/gc-txlog-segments-on-node!
                                            cluster-id
                                            logical-node)]))
                                  source-nodes)
            gc-results      (merge-with merge-gc-result
                                        best-results
                                        attempt-results)]
        (cond
          (wal-gap-realized? follower-next-lsn gc-results)
          gc-results

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep wal-gap-retry-sleep-ms)
            (recur gc-results))

          :else
          (throw
            (ex-info "Timed out forcing a real WAL gap before Jepsen rejoin bootstrap"
                     {:cluster-id cluster-id
                      :source-nodes source-nodes
                      :follower-next-lsn follower-next-lsn
                      :gc-results gc-results})))))))

(defn- choose-bootstrap-target!
  [test]
  (let [cluster-id (:datalevin/cluster-id test)
        {:keys [leader]} (local/wait-for-single-leader! cluster-id
                                                        converge-timeout-ms)
        {:keys [nodes live-nodes]} (local/cluster-state cluster-id)
        logical-nodes   (->> nodes (map :logical-node) sort vec)
        live-set        (set live-nodes)
        missing-nodes   (->> logical-nodes
                             (remove live-set)
                             sort
                             vec)]
    (cond
      (> (count missing-nodes) 1)
      (throw (ex-info "Jepsen rejoin-bootstrap expects at most one stopped node"
                      {:cluster-id cluster-id
                       :leader leader
                       :missing-nodes missing-nodes}))

      (= 1 (count missing-nodes))
      (let [logical-node (first missing-nodes)
            stopped-info (local/stopped-node-info cluster-id logical-node)]
        (when-not (map? stopped-info)
          (throw (ex-info "Missing stopped-node metadata for Jepsen bootstrap target"
                          {:cluster-id cluster-id
                           :logical-node logical-node
                           :leader leader})))
        {:leader leader
         :logical-node logical-node
         :stopped-info stopped-info
         :stopped-during-converge? false})

      :else
      (let [follower (->> live-nodes
                          sort
                          (remove #{leader})
                          first)]
        (when-not follower
          (throw (ex-info "Jepsen rejoin-bootstrap requires a live follower target"
                          {:cluster-id cluster-id
                           :leader leader
                           :live-nodes live-nodes})))
        (let [leader-lsn (local/effective-local-lsn cluster-id leader)]
          (local/wait-for-live-nodes-at-least-lsn! cluster-id
                                                   leader-lsn
                                                   converge-timeout-ms)
          (local/stop-node! cluster-id follower)
          (let [stopped-info (or (local/stopped-node-info cluster-id follower)
                                 {:effective-local-lsn leader-lsn})]
            {:leader leader
             :logical-node follower
             :stopped-info stopped-info
             :stopped-during-converge? true}))))))

(defn- force-snapshot-bootstrap!
  [test key-count]
  (let [cluster-id (:datalevin/cluster-id test)
        {:keys [leader logical-node stopped-info stopped-during-converge?]}
        (choose-bootstrap-target! test)
        baseline-lsn      (long (or (:effective-local-lsn stopped-info) 0))
        follower-next-lsn (unchecked-inc baseline-lsn)
        source-nodes      (->> (get-in (local/cluster-state cluster-id)
                                       [:live-nodes])
                               sort
                               vec)
        _                 (when (empty? source-nodes)
                            (throw (ex-info "No live Jepsen source nodes available for bootstrap"
                                            {:cluster-id cluster-id
                                             :logical-node logical-node})))
        _                 (write-register-batch-with-rolls! test
                                                            key-count
                                                            1000
                                                            wal-gap-writes-per-batch
                                                            wal-gap-write-sleep-ms)
        snapshot-1        (local/create-snapshots-on-nodes! cluster-id
                                                            source-nodes)
        _                 (write-register-batch-with-rolls! test
                                                            key-count
                                                            2000
                                                            wal-gap-writes-per-batch
                                                            wal-gap-write-sleep-ms)
        snapshot-2        (local/create-snapshots-on-nodes! cluster-id
                                                            source-nodes)
        min-snapshot-lsn  (long (or (get-in snapshot-1
                                            [leader :snapshot :applied-lsn])
                                    0))
        gc-results        (wait-for-real-wal-gap! cluster-id
                                                  source-nodes
                                                  follower-next-lsn)
        _                 (local/restart-node! cluster-id logical-node)
        bootstrap-state   (local/wait-for-follower-bootstrap! cluster-id
                                                              logical-node
                                                              min-snapshot-lsn
                                                              converge-timeout-ms)]
    {:bootstrap-state bootstrap-state
     :restarted-nodes [logical-node]
     :wal-gap {:target-node logical-node
               :source-nodes source-nodes
               :leader-at-stop leader
               :stopped-during-converge? stopped-during-converge?
               :stopped-node-info stopped-info
               :baseline-lsn baseline-lsn
               :follower-next-lsn follower-next-lsn
               :snapshots {:initial snapshot-1
                           :latest snapshot-2}
               :gc-results gc-results}}))

(defn- convergence-result
  [test key-count]
  (let [cluster-id        (:datalevin/cluster-id test)
        bootstrap-result  (force-snapshot-bootstrap! test key-count)
        restarted-nodes   (:restarted-nodes bootstrap-result)
        {:keys [leader]}  (local/wait-for-single-leader! cluster-id
                                                         converge-timeout-ms)
        expected-values   (leader-register-values test key-count)
        target-lsn        (local/effective-local-lsn cluster-id leader)
        lsn-wait          (try
                            {:caught-up? true
                             :snapshot (local/wait-for-live-nodes-at-least-lsn!
                                        cluster-id
                                        target-lsn
                                        converge-timeout-ms)}
                            (catch clojure.lang.ExceptionInfo e
                              (if (= "Timed out waiting for live nodes to catch up"
                                     (ex-message e))
                                {:caught-up? false
                                 :error (assoc (ex-data e)
                                               :message (ex-message e))}
                                (throw e))))
        live-node-state   (wait-for-expected-registers-on-live-nodes!
                            cluster-id
                            key-count
                            expected-values
                            converge-timeout-ms)]
    {:leader leader
     :expected expected-values
     :target-lsn target-lsn
     :caught-up? true
     :lsn-caught-up? (:caught-up? lsn-wait)
     :lsn-snapshot (:snapshot lsn-wait)
     :lsn-error (:error lsn-wait)
     :restarted-nodes restarted-nodes
     :bootstrap-state (:bootstrap-state bootstrap-result)
     :wal-gap (:wal-gap bootstrap-result)
     :nodes live-node-state}))

(defn- ensure-converged!
  [test key-count]
  (let [cluster-id (:datalevin/cluster-id test)]
    (if-let [result (get @converged-clusters cluster-id)]
      result
      (locking converged-clusters
        (if-let [result (get @converged-clusters cluster-id)]
          result
          (let [result (convergence-result test key-count)]
            (swap! converged-clusters assoc cluster-id result)
            result))))))

(defn- execute-op!
  [conn op]
  (let [[k v] (keyed-value op)]
    (case (:f op)
      :write
      (write-register! conn k v)

      :read
      (clojure.lang.MapEntry. k (register-value @conn k))

      :cas
      (cas-register! conn k v)

      ::unsupported)))

(defn- op-error
  [e]
  (if (= :transact/cas (:error (ex-data e)))
    :cas-failed
    (or (ex-message e)
        (.getName (class e)))))

(defn- mismatched-nodes
  [snapshot]
  (->> (:nodes snapshot)
       (keep (fn [[logical-node {:keys [ready? values] :as node-state}]]
               (when (or (not ready?)
                         (not= (:expected snapshot) values))
                 {:node logical-node
                  :ready? ready?
                  :values values
                  :expected (:expected snapshot)
                  :node-diagnostics (:node-diagnostics node-state)})))
       vec))

(defn- rejoin-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [converge-oks     (->> history
                                  (filter (fn [{:keys [type f value]}]
                                            (and (= :ok type)
                                                 (= :converge f)
                                                 (map? value))))
                                  (map :value)
                                  vec)
            converge-failures (->> history
                                   (filter (fn [{:keys [type f]}]
                                             (and (= :converge f)
                                                  (#{:fail :info} type))))
                                   (mapv (fn [{:keys [type error value]}]
                                           {:type type
                                            :error error
                                            :value value})))
            not-caught-up    (->> converge-oks
                                  (remove :caught-up?)
                                  vec)
            lsn-not-caught-up (->> converge-oks
                                   (remove :lsn-caught-up?)
                                   vec)
            mismatches       (->> converge-oks
                                  (mapcat mismatched-nodes)
                                  vec)]
        {:valid? (boolean
                  (and (seq converge-oks)
                       (empty? converge-failures)
                       (empty? not-caught-up)
                       (empty? lsn-not-caught-up)
                       (empty? mismatches)))
         :converge-count (count converge-oks)
         :failure-count (count converge-failures)
         :failure-samples (vec (take sample-limit converge-failures))
         :not-caught-up-count (count not-caught-up)
         :not-caught-up-samples (vec (take sample-limit not-caught-up))
         :lsn-not-caught-up-count (count lsn-not-caught-up)
         :lsn-not-caught-up-samples
         (vec (take sample-limit
                    (map #(select-keys % [:leader
                                          :target-lsn
                                          :lsn-snapshot
                                          :lsn-error
                                          :restarted-nodes])
                         lsn-not-caught-up)))
         :mismatch-count (count mismatches)
         :mismatch-samples (vec (take sample-limit mismatches))}))))

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
      (case (:f op)
        :converge
        (assoc op
               :type :ok
               :value (ensure-converged! test key-count))

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
                       :value result))))))
      (catch Throwable e
        (assoc op
               :type :fail
               :error (op-error e)
               :value (cond-> {:message (ex-message e)
                               :class (.getName (class e))}
                        (map? (ex-data e))
                        (merge (ex-data e)))))))

  (teardown! [this _test]
    this)

  (close! [_this _test]
    nil))

(defn- random-op
  [key-count]
  (let [choice (rand)]
    (cond
      (< choice 0.35) (read-op key-count)
      (< choice 0.70) (write-op key-count)
      :else (cas-op key-count))))

(defn workload
  [opts]
  (let [key-count (long (or (:key-count opts) 8))]
    {:client (->Client nil key-count)
     :generator (repeatedly #(random-op key-count))
     :final-generator {:type :invoke :f :converge}
     :checker (rejoin-checker)
     :datalevin/cluster-opts cluster-opts
     :schema schema}))
