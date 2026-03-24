(ns datalevin.jepsen.workload.degraded-rejoin
  (:require
   [datalevin.core :as d]
   [datalevin.ha :as dha]
   [datalevin.jepsen.local :as local]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [jepsen.generator :as gen]))

(def schema
  {:register/key   {:db/valueType :db.type/long
                    :db/unique :db.unique/identity}
   :register/value {:db/valueType :db.type/long}})

(def ^:private initial-value 0)
(def ^:private default-setup-timeout-ms 15000)
(def ^:private converge-timeout-ms 30000)
(def ^:private wal-gap-gc-timeout-ms 10000)
(def ^:private wal-gap-retry-sleep-ms 250)
(def ^:private write-sleep-ms 150)
(def ^:private writes-per-batch 4)
(def ^:private wal-gap-segment-max-ms 100)
(def ^:private wal-gap-replica-floor-ttl-ms 500)
(def ^:private sample-limit 10)
(def ^:private snapshot-unavailable-error-code
  :ha/follower-snapshot-unavailable)
(def ^:private snapshot-db-identity-mismatch-error-code
  :ha/follower-snapshot-db-identity-mismatch)
(def ^:private snapshot-missing-last-applied-lsn-error-code
  :ha/follower-snapshot-missing-last-applied-lsn)
(def ^:private snapshot-install-failed-error-code
  :ha/follower-snapshot-install-failed)
(def ^:private snapshot-checksum-mismatch-message
  "Copy checksum mismatch")
(defonce ^:private initialized-clusters (atom #{}))
(defonce ^:private scenario-runs (atom {}))
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

(defn- node-register-values
  [test logical-node key-count]
  (let [rows (local/local-query
               (:datalevin/cluster-id test)
               logical-node
               register-rows-query)]
    (when-not (= ::local/unavailable rows)
      (register-values-from-rows rows key-count))))

(defn- wait-for-register-values-on-nodes!
  [test logical-nodes expected-values timeout-ms key-count]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [last-snapshot nil]
      (let [snapshot (into {}
                           (map (fn [logical-node]
                                  [logical-node
                                   (node-register-values test
                                                         logical-node
                                                         key-count)]))
                           logical-nodes)]
        (cond
          (every? (fn [[_ values]]
                    (= expected-values values))
                  snapshot)
          snapshot

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info "Timed out waiting for Jepsen register convergence"
                          {:logical-nodes logical-nodes
                           :timeout-ms timeout-ms
                           :expected-values expected-values
                           :snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn- wait-for-initial-registers!
  [test key-count]
  (let [expected (vec (repeat (long key-count) (long initial-value)))]
    (wait-for-register-values-on-nodes!
      test
      (-> (:nodes test) sort vec)
      expected
      (local/workload-setup-timeout-ms (:datalevin/cluster-id test)
                                       default-setup-timeout-ms)
      key-count)))

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
          (wait-for-initial-registers! test key-count)
          (swap! initialized-clusters conj cluster-id))))))

(defn- write-register!
  [conn k v]
  (d/transact! conn [{:register/key (long k)
                      :register/value (long v)}])
  (clojure.lang.MapEntry. k (long v)))

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
                                                        converge-timeout-ms)]
    (local/effective-local-lsn cluster-id leader)))

(defn- realized-wal-gap-sources
  [follower-next-lsn gc-results]
  (->> gc-results
       (keep (fn [[logical-node {:keys [deleted-count] :as gc-result}]]
               (let [min-retained-lsn
                     (long (or (get-in gc-result [:after :min-retained-lsn]) 0))]
                 (when (and (pos? (long (or deleted-count 0)))
                            (> min-retained-lsn (long follower-next-lsn)))
                   logical-node))))
       sort
       vec))

(defn wal-gap-realized?
  [follower-next-lsn source-nodes gc-results]
  (= (vec (sort source-nodes))
     (realized-wal-gap-sources follower-next-lsn gc-results)))

(defn- snapshot-copy-failure-redefs
  [failure-mode orig-fetch orig-copy]
  (case failure-mode
    :snapshot-unavailable
    {#'dha/fetch-ha-endpoint-snapshot-copy!
     (fn [_ _ endpoint _]
       (throw (ex-info "forced snapshot source failure"
                       {:error snapshot-unavailable-error-code
                        :endpoint endpoint})))}

    :db-identity-mismatch
    {#'dha/fetch-ha-endpoint-snapshot-copy!
     (fn [db-name m endpoint dest-dir]
       (let [{:keys [copy-meta] :as result}
             (orig-fetch db-name m endpoint dest-dir)]
         (assoc result
                :copy-meta
                (assoc (or copy-meta {})
                       :db-name db-name
                       :db-identity "db-mismatch"))))}

    :manifest-corruption
    {#'dha/fetch-ha-endpoint-snapshot-copy!
     (fn [db-name m endpoint dest-dir]
       (let [{:keys [copy-meta] :as result}
             (orig-fetch db-name m endpoint dest-dir)]
         (assoc result
                :copy-meta
                (-> (or copy-meta {})
                    (assoc :db-name db-name
                           :db-identity (:ha-db-identity m))
                    (dissoc :snapshot-last-applied-lsn
                            :payload-last-applied-lsn)))))}

    :checksum-mismatch
    {#'dha/copy-ha-remote-store!
     (fn [remote-store dest-dir compact?]
       (let [copy-meta (orig-copy remote-store dest-dir compact?)]
         (throw (ex-info snapshot-checksum-mismatch-message
                         {:expected-checksum "forced-invalid-checksum"
                          :actual-checksum "forced-copy-checksum"}))
         copy-meta))}

    :copy-corruption
    {#'dha/fetch-ha-endpoint-snapshot-copy!
     (fn [db-name m endpoint dest-dir]
       (let [result (orig-fetch db-name m endpoint dest-dir)]
         (spit (str dest-dir "/data.mdb") "not-an-lmdb-file")
         result))}

    {}))

(defn- wait-for-real-wal-gap!
  [test key-count source-nodes follower-next-lsn start-value]
  (let [cluster-id (:datalevin/cluster-id test)
        deadline   (+ (System/currentTimeMillis) wal-gap-gc-timeout-ms)]
    (loop [next-start-value (long start-value)
           last-gc-results  nil]
      (let [gc-results (into {}
                             (map (fn [logical-node]
                                    [logical-node
                                     (local/gc-txlog-segments-on-node!
                                       cluster-id
                                       logical-node)]))
                             source-nodes)]
          (if (wal-gap-realized? follower-next-lsn source-nodes gc-results)
          {:gc-results gc-results
           :realized-source-nodes
           (realized-wal-gap-sources follower-next-lsn gc-results)}
          (if (< (System/currentTimeMillis) deadline)
            (let [leader-lsn (write-register-batch-with-rolls!
                               test
                               key-count
                               next-start-value
                               writes-per-batch
                               write-sleep-ms)
                  _          (local/wait-for-live-nodes-at-least-lsn!
                               cluster-id
                               leader-lsn
                               converge-timeout-ms)
                  _          (local/create-snapshots-on-nodes!
                               cluster-id
                               source-nodes)]
              (Thread/sleep (long wal-gap-retry-sleep-ms))
              (recur (unchecked-add (long next-start-value)
                                    (long writes-per-batch))
                     gc-results))
            {:gc-results gc-results
             :realized-source-nodes
             (realized-wal-gap-sources follower-next-lsn gc-results)
             :timed-out? true}))))))

(defn- run-scenario!
  [test key-count failure-mode]
  (ensure-registers-initialized! test key-count)
  (let [cluster-id (:datalevin/cluster-id test)
        {:keys [leader]} (local/wait-for-single-leader! cluster-id
                                                        converge-timeout-ms)
        initial-leader   leader
        live-nodes       (-> (local/cluster-state cluster-id) :live-nodes sort vec)
        degraded-node    (last (remove #{initial-leader} live-nodes))
        source-nodes     (->> live-nodes
                              (remove #{degraded-node})
                              sort
                              vec)
        baseline-lsn     (write-register-batch-with-rolls!
                           test
                           key-count
                           1000
                           writes-per-batch
                           write-sleep-ms)
        _                (local/wait-for-live-nodes-at-least-lsn!
                           cluster-id
                           baseline-lsn
                           converge-timeout-ms)
        baseline-values  (leader-register-values test key-count)
        _                (wait-for-register-values-on-nodes!
                           test
                           live-nodes
                           baseline-values
                           converge-timeout-ms
                           key-count)
        _                (local/stop-node! cluster-id degraded-node)
        follower-next-lsn
        (unchecked-inc
          (long (or (get-in (local/stopped-node-info cluster-id degraded-node)
                            [:effective-local-lsn])
                    0)))
        phase-2-lsn      (write-register-batch-with-rolls!
                           test
                           key-count
                           2000
                           writes-per-batch
                           write-sleep-ms)
        _                (local/wait-for-live-nodes-at-least-lsn!
                           cluster-id
                           phase-2-lsn
                           converge-timeout-ms)
        _                (local/create-snapshots-on-nodes! cluster-id source-nodes)
        phase-3-lsn      (write-register-batch-with-rolls!
                           test
                           key-count
                           3000
                           writes-per-batch
                           write-sleep-ms)
        _                (local/wait-for-live-nodes-at-least-lsn!
                           cluster-id
                           phase-3-lsn
                           converge-timeout-ms)
        _                (local/create-snapshots-on-nodes! cluster-id source-nodes)
        orig-fetch
        dha/fetch-ha-endpoint-snapshot-copy!
        orig-copy
        dha/copy-ha-remote-store!
        remote?          (:remote? (local/cluster-state cluster-id))
        wal-gap          (wait-for-real-wal-gap! test
                                                 key-count
                                                 source-nodes
                                                 follower-next-lsn
                                                 4000)
        _                (if remote?
                           (do
                             (local/set-node-snapshot-failpoint! cluster-id
                                                                 degraded-node
                                                                 failure-mode)
                             (try
                               (local/restart-node! cluster-id degraded-node)
                               (let [_             (local/with-leader-conn
                                                     test
                                                     schema
                                                     (fn [conn]
                                                       (write-register! conn 0 9000)))
                                     expected-live (leader-register-values test
                                                                          key-count)]
                                 (wait-for-register-values-on-nodes!
                                   test
                                   source-nodes
                                   expected-live
                                   converge-timeout-ms
                                   key-count))
                               (finally
                                 (local/clear-node-snapshot-failpoint!
                                  cluster-id
                                  degraded-node))))
                           (with-redefs-fn
                             (snapshot-copy-failure-redefs failure-mode
                                                           orig-fetch
                                                           orig-copy)
                             (fn []
                               (local/restart-node! cluster-id degraded-node)
                               (let [_             (local/with-leader-conn
                                                     test
                                                     schema
                                                     (fn [conn]
                                                       (write-register! conn 0 9000)))
                                     expected-live (leader-register-values test
                                                                          key-count)]
                                 (wait-for-register-values-on-nodes!
                                   test
                                   source-nodes
                                   expected-live
                                   converge-timeout-ms
                                   key-count)))))
        _                (local/with-leader-conn
                           test
                           schema
                           (fn [conn]
                             (write-register! conn 1 10000)))
        expected         (leader-register-values test key-count)
        nodes            (wait-for-register-values-on-nodes!
                           test
                           (-> (:nodes test) sort vec)
                           expected
                           converge-timeout-ms
                           key-count)]
    {:recovered? true
     :failure-mode failure-mode
     :degraded-node degraded-node
     :source-nodes source-nodes
     :wal-gap-realized?
     (wal-gap-realized? follower-next-lsn source-nodes (:gc-results wal-gap))
     :realized-source-nodes (:realized-source-nodes wal-gap)
     :wal-gap-timeout? (:timed-out? wal-gap)
     :expected expected
     :nodes (into {}
                  (map (fn [[logical-node values]]
                         [logical-node {:values values}]))
                  nodes)}))

(defn- scenario-result
  [test key-count failure-mode]
  (let [cluster-id (:datalevin/cluster-id test)
        scenario-key [cluster-id failure-mode]
        [result-promise owner?]
        (locking scenario-runs
          (if-let [p (get @scenario-runs scenario-key)]
            [p false]
            (let [p (promise)]
              (swap! scenario-runs assoc scenario-key p)
              [p true])))]
    (when owner?
      (try
        (deliver result-promise {:type :ok
                                 :value (run-scenario! test
                                                       key-count
                                                       failure-mode)})
        (catch Throwable e
          (deliver result-promise {:type :error
                                   :error e}))))
    (let [{:keys [type value error]} @result-promise]
      (case type
        :ok value
        (throw error)))))

(defn- scenario-op
  []
  {:type :invoke
   :f :exercise})

(defn- degraded-checker
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
            not-recovered (->> oks
                               (remove :recovered?)
                               vec)
            mismatches (->> oks
                            (mapcat (fn [{:keys [expected nodes]}]
                                      (keep (fn [[logical-node {:keys [values]}]]
                                              (when (not= expected values)
                                                {:logical-node logical-node
                                                 :expected expected
                                                 :actual values}))
                                            nodes)))
                            vec)]
        {:valid? (boolean
                  (and (seq oks)
                       (empty? failures)
                       (empty? not-recovered)
                       (empty? mismatches)))
         :exercise-count (count oks)
         :failure-count (count failures)
         :failure-samples (vec (take sample-limit failures))
         :not-recovered-count (count not-recovered)
         :not-recovered-samples (vec (take sample-limit not-recovered))
         :mismatch-count (count mismatches)
         :mismatch-samples (vec (take sample-limit mismatches))}))))

(defrecord Client [node key-count failure-mode]
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
        :exercise
        (assoc op
               :type :ok
               :value (scenario-result test key-count failure-mode))

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

(defn- failure-workload
  [opts failure-mode]
  (let [key-count (long (or (:key-count opts) 4))]
    {:client (->Client nil key-count failure-mode)
     :generator (gen/once (scenario-op))
     :checker (degraded-checker)
     :datalevin/cluster-opts cluster-opts
     :schema schema}))

(defn workload
  [opts]
  (failure-workload opts :snapshot-unavailable))

(defn db-identity-workload
  [opts]
  (failure-workload opts :db-identity-mismatch))

(defn manifest-corruption-workload
  [opts]
  (failure-workload opts :manifest-corruption))

(defn checksum-workload
  [opts]
  (failure-workload opts :checksum-mismatch))

(defn copy-corruption-workload
  [opts]
  (failure-workload opts :copy-corruption))
