(ns datalevin.jepsen.workload.membership-drift
  (:require
   [clojure.string :as str]
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [jepsen.generator :as gen]))

(def schema
  {:register/key {:db/valueType :db.type/long
                  :db/unique :db.unique/identity}
   :register/value {:db/valueType :db.type/long}})

(def ^:private initial-value 0)
(def ^:private default-setup-timeout-ms 15000)
(def ^:private converge-timeout-ms 30000)
(def ^:private live-converge-timeout-ms 60000)
(def ^:private sample-limit 10)
(def ^:private baseline-writes [[0 1000] [1 1001]])
(def ^:private recovered-writes [[0 2000] [1 2001]])
(def ^:private register-rows-query
  '[:find ?key ?value
    :where
    [?e :register/key ?key]
    [?e :register/value ?value]])
(defonce ^:private initialized-clusters (atom #{}))
(defonce ^:private scenario-runs (atom {}))

(defn- register-values-from-rows
  [rows key-count]
  (let [values-by-key (into {}
                            (map (fn [[k v]]
                                   [(long k) (long v)]))
                            rows)]
    (mapv (fn [k]
            (get values-by-key (long k)))
          (range (long key-count)))))

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
          (throw (ex-info "Timed out waiting for membership-drift register convergence"
                          {:logical-nodes logical-nodes
                           :timeout-ms timeout-ms
                           :expected-values expected-values
                           :snapshot snapshot
                           :previous-snapshot last-snapshot})))))))

(defn- wait-for-register-values-on-at-least-nodes!
  [test logical-nodes expected-values required-count timeout-ms key-count]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))
        required-count (long required-count)]
    (loop [last-snapshot nil]
      (let [snapshot (into {}
                           (map (fn [logical-node]
                                  [logical-node
                                   (node-register-values test
                                                         logical-node
                                                         key-count)]))
                           logical-nodes)
            matching (->> snapshot
                          (keep (fn [[logical-node values]]
                                  (when (= expected-values values)
                                    logical-node)))
                          sort
                          vec)]
        (cond
          (>= (count matching) required-count)
          {:snapshot snapshot
           :matching logical-nodes
           :matched-nodes matching}

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (throw (ex-info
                  "Timed out waiting for membership-drift quorum convergence"
                  {:logical-nodes logical-nodes
                   :timeout-ms timeout-ms
                   :required-count required-count
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

(defn- leader-register-values
  [test key-count]
  (local/with-leader-conn
    test
    schema
    (fn [conn]
      (register-values-from-rows
       (d/q register-rows-query @conn)
       key-count))))

(defn- write-register-pairs!
  [conn pairs]
  (d/transact! conn
               (mapv (fn [[k v]]
                       {:register/key (long k)
                        :register/value (long v)})
                     pairs))
  (mapv (fn [[k v]]
          (clojure.lang.MapEntry. (long k) (long v)))
        pairs))

(defn- drifted-ha-members
  [members target-node-id]
  (mapv (fn [member]
          (if (= target-node-id (:node-id member))
            (assoc member :endpoint
                   (str "127.0.0.1:" (+ 29000 (long target-node-id))))
            member))
        members))

(defn- restart-error
  [e]
  {:message (or (ex-message e)
                (.getName (class e)))
   :class (.getName (class e))
   :data (ex-data e)})

(defn- membership-hash-mismatch?
  [error]
  (or (= :ha/membership-hash-mismatch
         (get-in error [:data :error]))
      (= :ha/membership-hash-mismatch
         (get-in error [:data :err-data :error]))
      (some-> (:message error)
              str/lower-case
              (str/includes? "membership hash mismatch"))))

(defn- run-scenario!
  [test key-count]
  (ensure-registers-initialized! test key-count)
  (let [cluster-id (:datalevin/cluster-id test)
        leader-before (:leader (local/wait-for-single-leader!
                                cluster-id
                                live-converge-timeout-ms))
        live-before (-> (local/cluster-state cluster-id)
                        :live-nodes
                        sort
                        vec)
        drifted-node (first (remove #{leader-before} live-before))
        drifted-node-id (get-in (local/cluster-state cluster-id)
                                [:node-by-name drifted-node :node-id])
        original-members (get-in (local/cluster-state cluster-id)
                                 [:base-opts :ha-members])
        _ (local/with-leader-conn
            test
            schema
            (fn [conn]
              (write-register-pairs! conn baseline-writes)))
        baseline-target-lsn (local/effective-local-lsn cluster-id leader-before)
        _ (local/wait-for-live-nodes-at-least-lsn!
           cluster-id
           baseline-target-lsn
           live-converge-timeout-ms)
        baseline-expected (leader-register-values test key-count)
        _ (wait-for-register-values-on-nodes!
           test
           live-before
           baseline-expected
           live-converge-timeout-ms
           key-count)
        _ (local/stop-node! cluster-id drifted-node)
        drifted-members (drifted-ha-members original-members drifted-node-id)]
    (try
      (local/override-node-ha-opts! cluster-id drifted-node
                                    {:ha-members drifted-members})
      (let [failed-restart (try
                             (local/restart-node! cluster-id drifted-node)
                             nil
                             (catch Throwable e
                               (restart-error e)))
            _ (when-not failed-restart
                (throw (ex-info
                        "Membership-drift restart unexpectedly succeeded"
                        {:cluster-id cluster-id
                         :drifted-node drifted-node
                         :drifted-node-id drifted-node-id
                         :drifted-members drifted-members})))
            live-after-failed-restart
            (-> (local/cluster-state cluster-id)
                :live-nodes
                sort
                vec)
            _ (local/clear-node-ha-opts-override!
               cluster-id drifted-node)
            _ (local/restart-node! cluster-id drifted-node)
            leader-after (:leader (local/wait-for-single-leader!
                                   cluster-id
                                   converge-timeout-ms))
            live-after-restart
            (-> (local/cluster-state cluster-id)
                :live-nodes
                sort
                vec)
            _ (local/wait-for-live-nodes-at-least-lsn!
               cluster-id
               baseline-target-lsn
               converge-timeout-ms)
            expected baseline-expected
            nodes (wait-for-register-values-on-nodes!
                   test
                   live-after-restart
                   expected
                   converge-timeout-ms
                   key-count)
            rejoined-state (local/node-diagnostics cluster-id drifted-node)]
        {:leader-before leader-before
         :leader-after leader-after
         :drifted-node drifted-node
         :drifted-node-id drifted-node-id
         :restart-error failed-restart
         :live-before live-before
         :live-after-failed-restart live-after-failed-restart
         :live-after-restart live-after-restart
         :baseline-target-lsn baseline-target-lsn
         :target-lsn baseline-target-lsn
         :expected expected
         :rejoined-state rejoined-state
         :nodes (into {}
                      (map (fn [[logical-node values]]
                             [logical-node {:values values}]))
                      nodes)})
      (finally
        (local/clear-node-ha-opts-override! cluster-id drifted-node)))))

(defn- run-live-scenario!
  [test key-count]
  (ensure-registers-initialized! test key-count)
  (let [cluster-id (:datalevin/cluster-id test)
        leader-before (:leader (local/wait-for-single-leader!
                                cluster-id
                                converge-timeout-ms))
        live-before (-> (local/cluster-state cluster-id)
                        :live-nodes
                        sort
                        vec)
        drifted-node leader-before
        drifted-node-id (get-in (local/cluster-state cluster-id)
                                [:node-by-name drifted-node :node-id])
        original-members (get-in (local/cluster-state cluster-id)
                                 [:base-opts :ha-members])
        _ (local/with-leader-conn
            test
            schema
            (fn [conn]
              (write-register-pairs! conn baseline-writes)))
        baseline-target-lsn (local/effective-local-lsn cluster-id leader-before)
        _ (local/wait-for-live-nodes-at-least-lsn!
           cluster-id
           baseline-target-lsn
           converge-timeout-ms)
        baseline-expected (leader-register-values test key-count)
        _ (wait-for-register-values-on-nodes!
           test
           live-before
           baseline-expected
           converge-timeout-ms
           key-count)
        drifted-members (drifted-ha-members original-members drifted-node-id)]
    (try
      (let [drift-error (try
                          (local/assoc-opt-on-node-store!
                           cluster-id
                           drifted-node
                           :ha-members
                           drifted-members)
                          nil
                          (catch Throwable e
                            (restart-error e)))]
        (when-not drift-error
          (throw (ex-info
                  "Membership drift update on leader unexpectedly succeeded"
                  {:cluster-id cluster-id
                   :drifted-node drifted-node
                   :drifted-node-id drifted-node-id
                   :drifted-members drifted-members})))
        ;; Restore the drifted node's persisted membership offline so we don't
        ;; race the live node's own restart path after the mismatch.
        (local/stop-node! cluster-id drifted-node)
        (local/assoc-opt-on-stopped-node-store!
         cluster-id
         drifted-node
         :ha-members
         original-members)
        (local/restart-node! cluster-id drifted-node)
        (let [leader-after-restart (:leader (local/wait-for-single-leader!
                                             cluster-id
                                             live-converge-timeout-ms))
              _ (local/wait-for-nodes-at-least-lsn!
                 cluster-id
                 (distinct [leader-after-restart drifted-node])
                 baseline-target-lsn
                 live-converge-timeout-ms)
              _ (local/with-node-conn
                  test
                  leader-after-restart
                  schema
                  (fn [conn]
                    (write-register-pairs! conn recovered-writes)))
              leader-after (:leader (local/wait-for-single-leader!
                                     cluster-id
                                     live-converge-timeout-ms))
              target-lsn (local/effective-local-lsn cluster-id leader-after)
              live-after-restore
              (-> (local/cluster-state cluster-id)
                  :live-nodes
                  sort
                  vec)
              caught-up-state
              (local/wait-for-at-least-nodes-at-least-lsn!
               cluster-id
               live-after-restore
               target-lsn
               2
               live-converge-timeout-ms)
              expected (node-register-values test leader-after key-count)
              caught-up-nodes (:matched-nodes caught-up-state)
              {:keys [snapshot matched-nodes]}
              (wait-for-register-values-on-at-least-nodes!
               test
               live-after-restore
               expected
               2
               live-converge-timeout-ms
               key-count)
              recovered-state (local/node-diagnostics cluster-id drifted-node)]
          {:leader-before leader-before
           :leader-after leader-after
           :drifted-node drifted-node
           :drifted-node-id drifted-node-id
           :drift-error drift-error
           :live-before live-before
           :live-after-restore live-after-restore
           :caught-up-nodes caught-up-nodes
           :recovered-nodes matched-nodes
           :recovered-nodes-count (count matched-nodes)
           :target-lsn target-lsn
           :expected expected
           :recovered-state recovered-state
           :nodes (into {}
                        (map (fn [[logical-node values]]
                               [logical-node {:values values}]))
                        snapshot)}))
      (finally
        (try
          (if (get-in (local/cluster-state cluster-id)
                      [:servers drifted-node])
            (local/assoc-opt-on-node-store!
             cluster-id
             drifted-node
             :ha-members
             original-members)
            (local/assoc-opt-on-stopped-node-store!
             cluster-id
             drifted-node
             :ha-members
             original-members))
          (catch Throwable _
            nil))))))

(defn- scenario-result
  [test key-count]
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
                                 :value (run-scenario! test key-count)})
        (catch Throwable e
          (deliver result-promise {:type :error
                                   :error e}))))
    (let [{:keys [type value error]} @result-promise]
      (case type
        :ok value
        (throw error)))))

(defn- live-scenario-result
  [test key-count]
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
                                 :value (run-live-scenario! test key-count)})
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
            missing-drift-rejection
            (->> oks
                 (remove (comp membership-hash-mismatch? :restart-error))
                 vec)
            missing-rejoin
            (->> oks
                 (remove (fn [{:keys [live-after-failed-restart
                                      live-after-restart
                                      drifted-node]}]
                           (and (= 2 (count live-after-failed-restart))
                                (= 3 (count live-after-restart))
                                (contains? (set live-after-restart)
                                           drifted-node))))
                 vec)
            mismatches
            (->> oks
                 (mapcat (fn [{:keys [expected nodes]}]
                           (keep (fn [[logical-node {:keys [values]}]]
                                   (when (not= expected values)
                                     {:logical-node logical-node
                                      :expected expected
                                      :actual values})))
                           nodes))
                 vec)]
        {:valid? (boolean (and (seq oks)
                               (empty? failures)
                               (empty? missing-drift-rejection)
                               (empty? missing-rejoin)
                               (empty? mismatches)))
         :exercise-count (count oks)
         :failure-count (count failures)
         :failure-samples (vec (take sample-limit failures))
         :missing-drift-rejection-count (count missing-drift-rejection)
         :missing-drift-rejection-samples
         (vec (take sample-limit
                    (map #(select-keys % [:drifted-node
                                          :restart-error])
                         missing-drift-rejection)))
         :missing-rejoin-count (count missing-rejoin)
         :missing-rejoin-samples
         (vec (take sample-limit
                    (map #(select-keys % [:drifted-node
                                          :live-after-failed-restart
                                          :live-after-restart
                                          :rejoined-state])
                         missing-rejoin)))
         :mismatch-count (count mismatches)
         :mismatch-samples (vec (take sample-limit mismatches))}))))

(defn- live-checker*
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
            missing-drift-rejection
            (->> oks
                 (remove (comp membership-hash-mismatch? :drift-error))
                 vec)
            missing-recovery
            (->> oks
                 (remove (fn [{:keys [leader-before
                                      leader-after
                                      drifted-node
                                      live-before
                                      live-after-restore
                                      recovered-nodes-count
                                      recovered-nodes
                                      recovered-state]}]
                           (and (contains? (set live-before) drifted-node)
                                (contains? (set live-after-restore) drifted-node)
                                (= 3 (count live-after-restore))
                                (contains? (set recovered-nodes) drifted-node)
                                (>= (long (or recovered-nodes-count 0)) 2)
                                (some? leader-before)
                                (some? leader-after)
                                (some? recovered-state))))
                 vec)]
        {:valid? (boolean (and (seq oks)
                               (empty? failures)
                               (empty? missing-drift-rejection)
                               (empty? missing-recovery)))
         :exercise-count (count oks)
         :failure-count (count failures)
         :failure-samples (vec (take sample-limit failures))
         :missing-drift-rejection-count (count missing-drift-rejection)
         :missing-drift-rejection-samples
         (vec (take sample-limit
                    (map #(select-keys % [:drifted-node
                                          :drift-error])
                         missing-drift-rejection)))
         :missing-recovery-count (count missing-recovery)
         :missing-recovery-samples
         (vec (take sample-limit
                    (map #(select-keys % [:drifted-node
                                          :leader-before
                                          :leader-after
                                          :live-after-restore
                                          :recovered-nodes
                                          :recovered-nodes-count
                                          :recovered-state])
                         missing-recovery)))}))))

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
        :exercise
        (assoc op
               :type :ok
               :value (scenario-result test key-count))

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

(defrecord LiveClient [node key-count]
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
               :value (live-scenario-result test key-count))

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
  [opts]
  (let [key-count (long (or (:key-count opts) 4))]
    {:client (->Client nil key-count)
     :generator (gen/once (scenario-op))
     :checker (checker*)
     :schema schema}))

(defn live-workload
  [opts]
  (let [key-count (long (or (:key-count opts) 4))]
    {:client (->LiveClient nil key-count)
     :generator (gen/once (scenario-op))
     :checker (live-checker*)
     :schema schema}))
