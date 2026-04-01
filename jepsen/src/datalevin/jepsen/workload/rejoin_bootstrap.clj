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
(def ^:private default-setup-timeout-ms 15000)
(def ^:private converge-timeout-ms 30000)
(def ^:private wal-gap-gc-timeout-ms 10000)
(def ^:private wal-gap-write-timeout-ms 30000)
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

(defn- in-process-node-register-state
  [test logical-node key-count]
  (let [cluster-id (:datalevin/cluster-id test)
        rows       (local/local-query cluster-id
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

(defn- remote-node-register-state
  [test logical-node key-count]
  (let [cluster-id (:datalevin/cluster-id test)
        rows       (try
                     (local/with-node-conn
                       test
                       logical-node
                       schema
                       (fn [conn]
                         (d/q register-rows-query @conn)))
                     (catch Throwable _
                       ::local/unavailable))]
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
  [test key-count expected-values timeout-ms node-state-fn]
  (let [cluster-id (:datalevin/cluster-id test)
        deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [last-snapshot nil]
      (let [live-nodes (-> (local/cluster-state cluster-id) :live-nodes sort)
            snapshot   (into {}
                             (map (fn [logical-node]
                                    [logical-node
                                     (node-state-fn test
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
  [test key-count]
  (let [expected (vec (repeat (long key-count) (long initial-value)))]
    (wait-for-expected-registers-on-live-nodes!
      test
      key-count
      expected
      (local/workload-setup-timeout-ms (:datalevin/cluster-id test)
                                       default-setup-timeout-ms)
      in-process-node-register-state)))

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
          (wait-for-registers-visible-on-live-nodes! test key-count)
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
                                                        converge-timeout-ms)]
    ;; Forced WAL-gap bootstrap only needs an authoritative source to retain
    ;; the post-stop writes. Waiting for every remaining live follower to catch
    ;; up here can deadlock the setup under combo faults before the target node
    ;; is even restarted.
    (local/effective-local-lsn cluster-id leader)))

(defn- write-register-until-leader-lsn!
  [test key-count start-value min-leader-lsn]
  (let [cluster-id (:datalevin/cluster-id test)
        target-lsn (long min-leader-lsn)
        deadline   (+ (System/currentTimeMillis)
                      wal-gap-write-timeout-ms)]
    (loop [next-start-value (long start-value)
           leader-lsn
           (let [{:keys [leader]} (local/wait-for-single-leader! cluster-id
                                                                 converge-timeout-ms)]
             (long (local/effective-local-lsn cluster-id leader)))]
      (cond
        (>= leader-lsn target-lsn)
        {:leader-lsn leader-lsn
         :next-start-value next-start-value}

        (< (System/currentTimeMillis) deadline)
        (let [next-leader-lsn
              (write-register-batch-with-rolls! test
                                                key-count
                                                next-start-value
                                                wal-gap-writes-per-batch
                                                wal-gap-write-sleep-ms)]
          (recur (+ next-start-value wal-gap-writes-per-batch)
                 (long next-leader-lsn)))

        :else
        (throw
         (ex-info "Timed out advancing Jepsen leader LSN before forcing WAL gap"
                  {:cluster-id cluster-id
                   :target-lsn target-lsn
                   :leader-lsn leader-lsn
                   :start-value start-value
                   :next-start-value next-start-value
                   :timeout-ms wal-gap-write-timeout-ms}))))))

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

(defn- wal-gap-source?
  [follower-next-lsn result]
  (let [after            (or (:after result) result)
        min-retained-lsn (long (or (:min-retained-lsn after) 0))]
    (> min-retained-lsn (long follower-next-lsn))))

(defn- realized-wal-gap-sources
  [follower-next-lsn gc-results]
  (->> gc-results
       (keep (fn [[logical-node result]]
               (when (wal-gap-source? follower-next-lsn result)
                 logical-node)))
       sort
       vec))

(defn- wal-gap-realized?
  [follower-next-lsn gc-results]
  (boolean (seq (realized-wal-gap-sources follower-next-lsn gc-results))))

(defn- wait-for-real-wal-gap!
  [cluster-id source-nodes follower-next-lsn]
  (let [deadline (+ (System/currentTimeMillis) wal-gap-gc-timeout-ms)]
    (loop [best-results {}
           best-cleanup {}]
      (let [cleanup-results (into {}
                                 (map (fn [logical-node]
                                        [logical-node
                                         (local/clear-copy-backup-pins-on-node!
                                           cluster-id
                                           logical-node)]))
                                 source-nodes)
            attempt-results (into {}
                                  (map (fn [logical-node]
                                         [logical-node
                                          (local/gc-txlog-segments-on-node!
                                            cluster-id
                                            logical-node)]))
                                  source-nodes)
            gc-results      (merge-with merge-gc-result
                                        best-results
                                        attempt-results)
            cleanup-state   (merge-with
                             (fn [old new]
                               (if (seq (:remaining-pin-ids new))
                                 new
                                 old))
                             best-cleanup
                             cleanup-results)]
        (cond
          (wal-gap-realized? follower-next-lsn attempt-results)
          {:gc-results attempt-results
           :realized-source-nodes
           (realized-wal-gap-sources follower-next-lsn attempt-results)
           :copy-pin-cleanup cleanup-results}

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep wal-gap-retry-sleep-ms)
            (recur gc-results cleanup-state))

          :else
          (throw
            (ex-info "Timed out forcing a real WAL gap before Jepsen rejoin bootstrap"
                     {:cluster-id cluster-id
                      :source-nodes source-nodes
                      :follower-next-lsn follower-next-lsn
                      :realized-source-nodes
                      (realized-wal-gap-sources follower-next-lsn gc-results)
                      :gc-results gc-results
                      :copy-pin-cleanup cleanup-state
                      :last-gc-results attempt-results})))))))

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
          ;; The bootstrap target's stopped metadata records the exact follower
          ;; LSN. Requiring every live node to match the leader first is stricter
          ;; than the workload needs and breaks under sustained clock skew.
          (local/stop-node! cluster-id follower)
          (let [stopped-info (or (local/stopped-node-info cluster-id follower)
                                 {:effective-local-lsn leader-lsn})]
            {:leader leader
             :logical-node follower
             :stopped-info stopped-info
             :stopped-during-converge? true}))))))

(defn- stopped-node-baseline-lsn
  [stopped-info]
  (let [effective-lsn (long (or (:effective-local-lsn stopped-info) 0))
        runtime-lsn   (long (or (get-in stopped-info
                                        [:node-diagnostics
                                         :ha-local-last-applied-lsn])
                                0))
        next-lsn      (long (or (get-in stopped-info
                                        [:node-diagnostics
                                         :ha-follower-next-lsn])
                                0))
        resume-lsn    (long (max 0 (dec next-lsn)))]
    ;; The stopped-node snapshot captures both a conservative local floor and
    ;; the HA runtime's own replay cursor. Rejoin bootstrap must target the
    ;; higher of those values; otherwise the live sources can snapshot/GC below
    ;; what the restarted follower will actually require on resume.
    (long (max effective-lsn runtime-lsn resume-lsn))))

(defn- force-snapshot-bootstrap!
  [test key-count]
  (let [cluster-id (:datalevin/cluster-id test)
        {:keys [leader logical-node stopped-info stopped-during-converge?]}
        (choose-bootstrap-target! test)
        baseline-lsn      (stopped-node-baseline-lsn stopped-info)
        follower-next-lsn (unchecked-inc baseline-lsn)
        phase-1           (write-register-until-leader-lsn! test
                                                            key-count
                                                            1000
                                                            follower-next-lsn)
        source-nodes      (->> (get-in (local/cluster-state cluster-id)
                                       [:live-nodes])
                               (remove #{logical-node})
                               sort
                               vec)
        _                 (when (empty? source-nodes)
                            (throw (ex-info "No live Jepsen source nodes available for bootstrap"
                                            {:cluster-id cluster-id
                                             :logical-node logical-node})))
        _                 (local/wait-for-at-least-nodes-at-least-lsn!
                           cluster-id
                           source-nodes
                           follower-next-lsn
                           1
                           converge-timeout-ms)
        source-leader     (:leader (local/wait-for-single-leader! cluster-id
                                                                  converge-timeout-ms))
        snapshot-1        (local/create-snapshots-on-nodes! cluster-id
                                                            source-nodes)
        min-snapshot-lsn  (apply min
                                 (map (fn [source-node]
                                        (long (or (get-in snapshot-1
                                                          [source-node
                                                           :snapshot
                                                           :applied-lsn])
                                                  0)))
                                      source-nodes))
        phase-2-target-lsn (long (max (unchecked-inc follower-next-lsn)
                                      (+ min-snapshot-lsn
                                         wal-gap-writes-per-batch)))
        phase-2           (write-register-until-leader-lsn! test
                                                            key-count
                                                            (:next-start-value phase-1)
                                                            phase-2-target-lsn)
        _                 (local/wait-for-at-least-nodes-at-least-lsn!
                           cluster-id
                           source-nodes
                           phase-2-target-lsn
                           1
                           converge-timeout-ms)
        snapshot-2        (local/create-snapshots-on-nodes! cluster-id
                                                            source-nodes)
        required-snapshot-lsn
        (apply min
               (map (fn [source-node]
                      (long (or (get-in snapshot-2
                                        [source-node
                                         :snapshot
                                         :applied-lsn])
                                0)))
                    source-nodes))
        {:keys [gc-results realized-source-nodes copy-pin-cleanup]}
        (wait-for-real-wal-gap! cluster-id
                                source-nodes
                                follower-next-lsn)
        _                 (local/restart-node! cluster-id logical-node)
        bootstrap-state
        (try
          (local/wait-for-follower-bootstrap! cluster-id
                                             logical-node
                                             required-snapshot-lsn
                                             converge-timeout-ms)
          (catch clojure.lang.ExceptionInfo e
            (let [last-state    (:last-state (ex-data e))
                  applied-lsn   (long (or (:ha-local-last-applied-lsn last-state)
                                          0))
                  effective-lsn (long (or (:ha-effective-local-lsn last-state)
                                          applied-lsn))
                  next-lsn      (long (or (:ha-follower-next-lsn last-state)
                                          0))]
              (if (and (map? last-state)
                       (#{:follower nil} (:ha-role last-state))
                       (nil? (:ha-follower-last-error last-state))
                       (>= effective-lsn required-snapshot-lsn)
                       (>= next-lsn (unchecked-inc required-snapshot-lsn)))
                (assoc last-state
                       :jepsen-bootstrap-inferred? true
                       :ha-follower-last-bootstrap-ms
                       (or (:ha-follower-last-bootstrap-ms last-state)
                           (:ha-follower-last-sync-ms last-state))
                       :ha-follower-bootstrap-source-endpoint
                       (or (:ha-follower-bootstrap-source-endpoint last-state)
                           (:ha-follower-source-endpoint last-state))
                       :ha-follower-bootstrap-snapshot-last-applied-lsn
                       required-snapshot-lsn)
                (throw e)))))]
    {:bootstrap-state bootstrap-state
     :restarted-nodes [logical-node]
     :wal-gap {:target-node logical-node
               :source-nodes source-nodes
               :leader-at-stop leader
               :leader-at-bootstrap source-leader
               :stopped-during-converge? stopped-during-converge?
               :stopped-node-info stopped-info
               :baseline-lsn baseline-lsn
               :follower-next-lsn follower-next-lsn
               :required-snapshot-lsn required-snapshot-lsn
               :phase-1 phase-1
               :phase-2 phase-2
               :snapshots {:initial snapshot-1
                           :latest snapshot-2}
               :realized-source-nodes realized-source-nodes
               :copy-pin-cleanup copy-pin-cleanup
               :gc-results gc-results}}))

(defn- convergence-result
  [test key-count]
  (let [cluster-id        (:datalevin/cluster-id test)
        bootstrap-result  (force-snapshot-bootstrap! test key-count)
        restarted-nodes   (:restarted-nodes bootstrap-result)
        restarted-set     (set restarted-nodes)
        {:keys [leader]}  (local/wait-for-single-leader! cluster-id
                                                         converge-timeout-ms)
        expected-values   (leader-register-values test key-count)
        target-lsn        (local/effective-local-lsn cluster-id leader)
        _                 (try
                            (local/wait-for-live-nodes-at-least-lsn!
                             cluster-id
                             target-lsn
                             converge-timeout-ms)
                            (catch clojure.lang.ExceptionInfo e
                              (when-not (= "Timed out waiting for live nodes to catch up"
                                           (ex-message e))
                                (throw e))))
        live-node-state   (wait-for-expected-registers-on-live-nodes!
                            test
                            key-count
                            expected-values
                            converge-timeout-ms
                            (fn [test logical-node key-count]
                              ((if (contains? restarted-set logical-node)
                                 remote-node-register-state
                                 in-process-node-register-state)
                               test
                               logical-node
                               key-count)))]
    {:leader leader
     :expected expected-values
     :caught-up? true
     :restarted-nodes restarted-nodes
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
            mismatches       (->> converge-oks
                                  (mapcat mismatched-nodes)
                                  vec)]
        {:valid? (boolean
                  (and (seq converge-oks)
                       (empty? converge-failures)
                       (empty? mismatches)))
         :converge-count (count converge-oks)
         :failure-count (count converge-failures)
         :failure-samples (vec (take sample-limit converge-failures))
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
