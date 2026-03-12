(ns datalevin.jepsen.workload.fencing-retry
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [datalevin.util :as u]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [jepsen.generator :as gen])
  (:import
   [java.util UUID]))

(def schema
  {:register/key   {:db/valueType :db.type/long
                    :db/unique :db.unique/identity}
   :register/value {:db/valueType :db.type/long}})

(def ^:private initial-value 0)
(def ^:private default-setup-timeout-ms 15000)
(def ^:private converge-timeout-ms 30000)
(def ^:private blocked-leader-timeout-ms 4000)
(def ^:private sample-limit 10)
(def ^:private hook-timeout-ms 1000)
(def ^:private hook-retries 2)
(def ^:private default-data-nodes ["n1" "n2"])
(def ^:private default-control-nodes ["n1" "n2" "n3"])
(def ^:private baseline-writes [[0 1000] [1 1001]])
(def ^:private failover-writes [[2 2000] [3 2001]])
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
          (throw (ex-info "Timed out waiting for fencing-retry register convergence"
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

(defn- topology-snapshot
  [test]
  (let [cluster        (local/cluster-state (:datalevin/cluster-id test))
        voters         (vec (get-in cluster [:base-opts :ha-control-plane :voters]))
        members        (vec (get-in cluster [:base-opts :ha-members]))
        data-nodes     (vec (:data-node-names cluster))
        control-nodes  (vec (:control-node-names cluster))]
    {:data-nodes data-nodes
     :control-nodes control-nodes
     :control-only-node-names (vec (:control-only-node-names cluster))
     :ha-members members
     :voters voters
     :promotable-voters (->> voters
                             (filter :promotable?)
                             vec)
     :non-promotable-voters (->> voters
                                 (remove :promotable?)
                                 vec)}))

(defn- valid-topology?
  [{:keys [data-nodes control-nodes control-only-node-names
           ha-members promotable-voters non-promotable-voters]}]
  (let [member-ids     (set (map :node-id ha-members))
        promotable-ids (set (keep :ha-node-id promotable-voters))]
    (and (= 2 (count data-nodes))
         (= 3 (count control-nodes))
         (= 1 (count control-only-node-names))
         (= 2 (count ha-members))
         (= 2 (count promotable-voters))
         (= 1 (count non-promotable-voters))
         (= member-ids promotable-ids))))

(defn- hook-mode-file
  [state-dir]
  (str state-dir u/+separator+ "mode.txt"))

(defn- hook-log-file
  [state-dir]
  (str state-dir u/+separator+ "hook.log"))

(defn- write-hook-mode!
  [state-dir mode]
  (spit (hook-mode-file state-dir) (name mode)))

(defn- hook-command
  [state-dir]
  (let [log-file  (hook-log-file state-dir)
        mode-file (hook-mode-file state-dir)]
    ["/bin/sh" "-c"
     (str "mode=$(cat \"$2\" 2>/dev/null || true); "
          "if [ -z \"$mode\" ]; then mode=success; fi; "
          "printf '%s,%s,%s,%s,%s,%s,%s,%s\\n' "
          "\"$DTLV_DB_NAME\" "
          "\"$DTLV_FENCE_OP_ID\" "
          "\"$DTLV_TERM_OBSERVED\" "
          "\"$DTLV_TERM_CANDIDATE\" "
          "\"$DTLV_NEW_LEADER_NODE_ID\" "
          "\"$DTLV_OLD_LEADER_NODE_ID\" "
          "\"$DTLV_OLD_LEADER_ENDPOINT\" "
          "\"$mode\" >> \"$1\"; "
          "if [ \"$mode\" = fail ]; then exit 7; fi; "
          "exit 0")
     "fence-hook"
     log-file
     mode-file]))

(defn- hook-log-entries
  [state-dir]
  (let [log-file (io/file (hook-log-file state-dir))]
    (if (.exists log-file)
      (->> (slurp log-file)
           str/split-lines
           (keep (fn [line]
                   (let [[db-name fence-op-id observed-term candidate-term
                          new-leader-node-id old-leader-node-id
                          old-leader-endpoint mode]
                         (str/split line #"," 8)]
                     (when (and (seq db-name)
                                (seq fence-op-id)
                                (seq observed-term)
                                (seq candidate-term)
                                (seq new-leader-node-id)
                                (seq mode))
                       (let [candidate-node-id
                             (Long/parseLong new-leader-node-id)]
                         {:db-name db-name
                          :fence-op-id fence-op-id
                          :candidate-node-id candidate-node-id
                          :new-leader-node-id candidate-node-id
                          :candidate-term (Long/parseLong candidate-term)
                          :observed-term (Long/parseLong observed-term)
                          :old-leader-node-id
                          (when (seq old-leader-node-id)
                            (Long/parseLong old-leader-node-id))
                          :old-leader-endpoint (or old-leader-endpoint "")
                          :mode mode
                          :raw line})))))
           vec)
      [])))

(defn- retry-groups
  [entries]
  (->> entries
       (group-by :fence-op-id)
       (map (fn [[fence-op-id grouped]]
              {:fence-op-id fence-op-id
               :attempt-count (count grouped)
               :candidate-node-ids (->> grouped
                                        (map :candidate-node-id)
                                        set)
               :candidate-terms (->> grouped
                                     (map :candidate-term)
                                     set)
               :observed-terms (->> grouped
                                    (map :observed-term)
                                    set)
               :modes (->> grouped
                           (map :mode)
                           set)
               :entries (vec grouped)}))
       (sort-by (juxt (comp - :attempt-count) :fence-op-id))
       vec))

(defn- wait-for-failed-retry-group!
  [state-dir candidate-node-id min-attempts timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [last-groups nil]
      (let [groups (retry-groups (hook-log-entries state-dir))
            group  (some (fn [{:keys [attempt-count candidate-node-ids modes]
                               :as retry-group}]
                           (when (and (>= attempt-count min-attempts)
                                      (= #{(long candidate-node-id)}
                                         candidate-node-ids)
                                      (= #{"fail"} modes))
                             retry-group))
                         groups)]
        (cond
          group
          group

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep 250)
            (recur groups))

          :else
          (throw (ex-info "Timed out waiting for fencing retry group"
                          {:candidate-node-id candidate-node-id
                           :min-attempts min-attempts
                           :timeout-ms timeout-ms
                           :retry-groups groups
                           :previous-retry-groups last-groups})))))))

(defn- wait-for-promotion-failure!
  [cluster-id logical-node timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [last-state nil]
      (let [state (local/node-diagnostics cluster-id logical-node)]
        (cond
          (= :fencing-failed (:ha-promotion-last-failure state))
          state

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep 250)
            (recur (or state last-state)))

          :else
          (throw (ex-info "Timed out waiting for fencing promotion failure"
                          {:cluster-id cluster-id
                           :logical-node logical-node
                           :timeout-ms timeout-ms
                           :last-state last-state})))))))

(defn- wait-for-success-hook-entry!
  [state-dir candidate-node-id timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [last-entries nil]
      (let [entries (hook-log-entries state-dir)
            entry   (->> entries
                         (filter (fn [{entry-node-id :candidate-node-id
                                       :keys [mode]}]
                                   (and (= (long candidate-node-id)
                                           (long entry-node-id))
                                        (= "success" mode))))
                         last)]
        (cond
          entry
          entry

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep 250)
            (recur entries))

          :else
          (throw (ex-info "Timed out waiting for successful fencing hook"
                          {:candidate-node-id candidate-node-id
                           :timeout-ms timeout-ms
                           :hook-log-entries entries
                           :previous-hook-log-entries last-entries})))))))

(defn- valid-success-hook-entry?
  [{:keys [db-name stopped-node-id stopped-node-endpoint
           candidate-node-id leader-after-id success-hook-entry]}]
  (let [{entry-db-name :db-name
         :keys [fence-op-id observed-term candidate-term
                new-leader-node-id old-leader-node-id old-leader-endpoint
                mode]}
        success-hook-entry
        observed-term      (long (or observed-term -1))
        candidate-term     (long (or candidate-term -1))
        candidate-node-id  (long (or candidate-node-id -1))
        leader-after-id    (long (or leader-after-id -1))
        stopped-node-id    (long (or stopped-node-id -1))
        new-leader-node-id (long (or new-leader-node-id -1))
        old-leader-node-id (long (or old-leader-node-id -1))]
    (and (= "success" mode)
         (= db-name entry-db-name)
         (= candidate-node-id new-leader-node-id)
         (= candidate-node-id leader-after-id)
         (= stopped-node-id old-leader-node-id)
         (= stopped-node-endpoint old-leader-endpoint)
         (= (str db-name ":" observed-term ":" candidate-node-id)
            fence-op-id)
         (= (inc observed-term) candidate-term))))

(defn- run-scenario!
  [test key-count state-dir]
  (ensure-registers-initialized! test key-count)
  (write-hook-mode! state-dir :success)
  (let [cluster-id          (:datalevin/cluster-id test)
        cluster             (local/cluster-state cluster-id)
        topology            (topology-snapshot test)
        live-before         (-> cluster
                                :live-nodes
                                sort
                                vec)
        leader-before       (:leader (local/wait-for-single-leader!
                                      cluster-id
                                      converge-timeout-ms))
        candidate-node      (first (remove #{leader-before} live-before))
        candidate-node-id   (get-in cluster
                                    [:node-by-name candidate-node :node-id])
        stopped-node-id     (get-in cluster
                                    [:node-by-name leader-before :node-id])
        stopped-node-endpoint
        (local/endpoint-for-node cluster-id leader-before)
        _                   (local/with-leader-conn
                              test
                              schema
                              (fn [conn]
                                (write-register-pairs! conn baseline-writes)))
        baseline-target-lsn (local/effective-local-lsn cluster-id leader-before)
        _                   (local/wait-for-live-nodes-at-least-lsn!
                              cluster-id
                              baseline-target-lsn
                              converge-timeout-ms)
        _                   (wait-for-register-values-on-nodes!
                              test
                              live-before
                              (leader-register-values test key-count)
                              converge-timeout-ms
                              key-count)
        _                   (write-hook-mode! state-dir :fail)
        _                   (local/stop-node! cluster-id leader-before)
        live-after-stop     (-> (local/cluster-state cluster-id)
                                :live-nodes
                                sort
                                vec)
        failed-retry-group  (wait-for-failed-retry-group!
                              state-dir
                              candidate-node-id
                              (inc hook-retries)
                              converge-timeout-ms)
        failed-state        (wait-for-promotion-failure!
                              cluster-id
                              candidate-node
                              converge-timeout-ms)
        blocked-snapshot    (local/maybe-wait-for-single-leader
                              cluster-id
                              blocked-leader-timeout-ms)
        _                   (write-hook-mode! state-dir :success)
        success-hook-entry  (wait-for-success-hook-entry!
                              state-dir
                              candidate-node-id
                              converge-timeout-ms)
        leader-after        (:leader (local/wait-for-single-leader!
                                      cluster-id
                                      converge-timeout-ms))
        leader-after-id     (get-in (local/cluster-state cluster-id)
                                    [:node-by-name leader-after :node-id])
        _                   (local/with-leader-conn
                              test
                              schema
                              (fn [conn]
                                (write-register-pairs! conn failover-writes)))
        target-lsn          (local/effective-local-lsn cluster-id leader-after)
        _                   (local/wait-for-live-nodes-at-least-lsn!
                              cluster-id
                              target-lsn
                              converge-timeout-ms)
        expected            (leader-register-values test key-count)
        nodes               (wait-for-register-values-on-nodes!
                              test
                              live-after-stop
                              expected
                              converge-timeout-ms
                              key-count)]
    {:topology topology
     :db-name (:db-name test)
     :live-before live-before
     :live-after-stop live-after-stop
     :leader-before leader-before
     :stopped-node leader-before
     :stopped-node-id stopped-node-id
     :stopped-node-endpoint stopped-node-endpoint
     :candidate-node candidate-node
     :candidate-node-id candidate-node-id
     :leader-during-fencing-failure (some-> blocked-snapshot :leader)
     :failed-state failed-state
     :failed-retry-group failed-retry-group
     :leader-after leader-after
     :leader-after-id leader-after-id
     :success-hook-entry success-hook-entry
     :target-lsn target-lsn
     :expected expected
     :nodes (into {}
                  (map (fn [[logical-node values]]
                         [logical-node {:values values}]))
                  nodes)}))

(defn- scenario-result
  [test key-count state-dir]
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
                                 :value (run-scenario! test key-count state-dir)})
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
            invalid-topology
            (->> oks
                 (remove (comp valid-topology? :topology))
                 vec)
            missing-retry-group
            (->> oks
                 (remove (fn [{:keys [candidate-node-id failed-retry-group]}]
                           (and (map? failed-retry-group)
                                (>= (long (or (:attempt-count failed-retry-group)
                                              0))
                                    (inc hook-retries))
                                (= #{(long candidate-node-id)}
                                   (:candidate-node-ids failed-retry-group))
                                (= #{"fail"} (:modes failed-retry-group)))))
                 vec)
            missing-fencing-failure
            (->> oks
                 (remove (fn [{:keys [failed-state]}]
                           (= :fencing-failed
                              (:ha-promotion-last-failure failed-state))))
                 vec)
            unexpected-leader-during-failure
            (->> oks
                 (remove (comp nil? :leader-during-fencing-failure))
                 vec)
            failover-mismatches
            (->> oks
                 (remove (fn [{:keys [leader-before candidate-node leader-after
                                      live-after-stop]}]
                           (and (= [candidate-node] live-after-stop)
                                (= candidate-node leader-after)
                                (not= leader-before leader-after)
                                (contains? #{"n1" "n2"} leader-after))))
                 vec)
            invalid-success-hook
            (->> oks
                 (remove valid-success-hook-entry?)
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
                               (empty? invalid-topology)
                               (empty? missing-retry-group)
                               (empty? missing-fencing-failure)
                               (empty? unexpected-leader-during-failure)
                               (empty? failover-mismatches)
                               (empty? invalid-success-hook)
                               (empty? mismatches)))
         :exercise-count (count oks)
         :failure-count (count failures)
         :failure-samples (vec (take sample-limit failures))
         :invalid-topology-count (count invalid-topology)
         :invalid-topology-samples
         (vec (take sample-limit
                    (map #(select-keys % [:topology])
                         invalid-topology)))
         :missing-retry-group-count (count missing-retry-group)
         :missing-retry-group-samples
         (vec (take sample-limit
                    (map #(select-keys % [:candidate-node
                                          :candidate-node-id
                                          :failed-retry-group])
                         missing-retry-group)))
         :missing-fencing-failure-count (count missing-fencing-failure)
         :missing-fencing-failure-samples
         (vec (take sample-limit
                    (map #(select-keys % [:candidate-node
                                          :failed-state])
                         missing-fencing-failure)))
         :unexpected-leader-during-failure-count
         (count unexpected-leader-during-failure)
         :unexpected-leader-during-failure-samples
         (vec (take sample-limit
                    (map #(select-keys % [:leader-before
                                          :leader-during-fencing-failure])
                         unexpected-leader-during-failure)))
         :failover-mismatch-count (count failover-mismatches)
         :failover-mismatch-samples
         (vec (take sample-limit
                    (map #(select-keys % [:leader-before
                                          :candidate-node
                                          :leader-after
                                          :live-after-stop])
                         failover-mismatches)))
         :invalid-success-hook-count (count invalid-success-hook)
         :invalid-success-hook-samples
         (vec (take sample-limit
                    (map #(select-keys % [:db-name
                                          :stopped-node
                                          :stopped-node-id
                                          :stopped-node-endpoint
                                          :candidate-node
                                          :candidate-node-id
                                          :leader-after
                                          :leader-after-id
                                          :success-hook-entry])
                         invalid-success-hook)))
         :mismatch-count (count mismatches)
         :mismatch-samples (vec (take sample-limit mismatches))}))))

(defrecord Client [node key-count state-dir]
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
               :value (scenario-result test key-count state-dir))

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
  (let [key-count (long (or (:key-count opts) 4))
        state-dir (u/tmp-dir (str "jepsen-fencing-retry-" (UUID/randomUUID)))]
    (u/create-dirs state-dir)
    (write-hook-mode! state-dir :success)
    {:client (->Client nil key-count state-dir)
     :generator (gen/once (scenario-op))
     :checker (checker*)
     :schema schema
     :nodes default-data-nodes
     :datalevin/control-nodes default-control-nodes
     :datalevin/cluster-opts
     {:ha-fencing-hook {:cmd (hook-command state-dir)
                        :timeout-ms hook-timeout-ms
                        :retries hook-retries
                        :retry-delay-ms 0}}}))
