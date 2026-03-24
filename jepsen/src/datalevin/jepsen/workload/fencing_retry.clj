(ns datalevin.jepsen.workload.fencing-retry
  (:require
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

(defn- hook-mode-file
  [state-dir]
  (str state-dir u/+separator+ "mode.txt"))

(defn- write-hook-mode!
  [state-dir mode]
  (spit (hook-mode-file state-dir) (name mode)))

(defn- hook-command
  [state-dir]
  (let [mode-file (hook-mode-file state-dir)]
    ["/bin/sh" "-c"
     (str "mode=$(cat \"$2\" 2>/dev/null || true); "
          "if [ -z \"$mode\" ]; then mode=success; fi; "
          "if [ \"$mode\" = fail ]; then exit 7; fi; "
          "exit 0")
     "fence-hook"
     mode-file]))

(def ^:private blocked-write-failure-markers
  ["HA write admission rejected"
   "Timed out waiting for single leader"
   "Timeout in making request"
   "Unable to connect to server:"
   "Connection refused"])

(defn- blocked-write-error?
  [e]
  (or (local/transport-failure? e)
      (when-let [message (ex-message e)]
        (some #(str/includes? message %)
              blocked-write-failure-markers))))

(defn- wait-for-write-blocked!
  [test logical-node pairs timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [attempt-count 0]
      (let [result (try
                     (local/with-node-conn
                       test
                       logical-node
                       schema
                       (fn [conn]
                         (write-register-pairs! conn pairs)
                         :committed))
                     (catch Throwable e
                       e))]
        (cond
          (and (instance? Throwable result)
               (blocked-write-error? result))
          {:logical-node logical-node
           :attempt-count (inc attempt-count)
           :last-error (or (ex-message result)
                           (.getName (class result)))
           :last-error-class (.getName (class result))}

          (= :committed result)
          (throw (ex-info "Write unexpectedly committed before fencing blocked it"
                          {:logical-node logical-node
                           :pairs pairs
                           :attempt-count attempt-count}))

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep 250)
            (recur (inc attempt-count)))

          (instance? Throwable result)
          (throw result)

          :else
          (throw (ex-info "Timed out waiting for fencing-retry write rejection"
                          {:logical-node logical-node
                           :pairs pairs
                           :attempt-count attempt-count
                           :result result})))))))

(defn- run-scenario!
  [test key-count state-dir]
  (ensure-registers-initialized! test key-count)
  ((if (:remote? (local/cluster-state (:datalevin/cluster-id test)))
     local/set-fencing-hook-mode!
     (fn [cluster-id mode]
       (when state-dir
         (write-hook-mode! state-dir mode))))
   (:datalevin/cluster-id test)
   :success)
  (let [cluster-id          (:datalevin/cluster-id test)
        cluster             (local/cluster-state cluster-id)
        live-before         (-> cluster
                                :live-nodes
                                sort
                                vec)
        leader-before       (:leader (local/wait-for-single-leader!
                                      cluster-id
                                      converge-timeout-ms))
        candidate-node      (first (remove #{leader-before} live-before))
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
        _                   ((if (:remote? cluster)
                               local/set-fencing-hook-mode!
                               (fn [cluster-id mode]
                                 (when state-dir
                                   (write-hook-mode! state-dir mode))))
                             cluster-id
                             :fail)
        _                   (local/stop-node! cluster-id leader-before)
        live-after-stop     (-> (local/cluster-state cluster-id)
                                :live-nodes
                                sort
                                vec)
        blocked-write       (wait-for-write-blocked!
                              test
                              candidate-node
                              [[0 1500]]
                              blocked-leader-timeout-ms)
        _                   ((if (:remote? cluster)
                               local/set-fencing-hook-mode!
                               (fn [cluster-id mode]
                                 (when state-dir
                                   (write-hook-mode! state-dir mode))))
                             cluster-id
                             :success)
        leader-after        (:leader (local/wait-for-single-leader!
                                      cluster-id
                                      converge-timeout-ms))
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
    {:live-before live-before
     :live-after-stop live-after-stop
     :leader-before leader-before
     :stopped-node leader-before
     :candidate-node candidate-node
     :blocked-write blocked-write
     :leader-after leader-after
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
            blocked-write-failures
            (->> oks
                 (remove (fn [{:keys [blocked-write]}]
                           (and (map? blocked-write)
                                (pos? (long (or (:attempt-count blocked-write)
                                                0))))))
                 vec)
            missing-failover
            (->> oks
                 (remove (fn [{:keys [leader-before leader-after nodes]}]
                           (and (string? leader-after)
                                (not= leader-before leader-after)
                                (contains? (set (keys nodes))
                                           leader-after))))
                 vec)
            mismatches
            (->> oks
                 (mapcat (fn [{:keys [expected nodes]}]
                           (keep (fn [[logical-node {:keys [values]}]]
                                   (when (not= expected values)
                                     {:logical-node logical-node
                                      :expected expected
                                      :actual values}))
                                 nodes)))
                 vec)]
        {:valid? (boolean (and (seq oks)
                               (empty? failures)
                               (empty? blocked-write-failures)
                               (empty? missing-failover)
                               (empty? mismatches)))
         :exercise-count (count oks)
         :failure-count (count failures)
         :failure-samples (vec (take sample-limit failures))
         :blocked-write-failure-count (count blocked-write-failures)
         :blocked-write-failure-samples
         (vec (take sample-limit
                    (map #(select-keys % [:leader-before
                                          :candidate-node
                                          :blocked-write])
                         blocked-write-failures)))
         :missing-failover-count (count missing-failover)
         :missing-failover-samples
         (vec (take sample-limit
                    (map #(select-keys % [:leader-before
                                          :leader-after
                                          :nodes])
                         missing-failover)))
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
        remote?   (true? (:datalevin/remote-runner? opts))
        state-dir (when-not remote?
                    (u/tmp-dir (str "jepsen-fencing-retry-" (UUID/randomUUID))))]
    (when state-dir
      (u/create-dirs state-dir)
      (write-hook-mode! state-dir :success))
    (cond-> {:client (->Client nil key-count state-dir)
     :generator (gen/once (scenario-op))
     :checker (checker*)
     :schema schema
     :nodes default-data-nodes
     :datalevin/control-nodes default-control-nodes
     :datalevin/cluster-opts
     {:ha-fencing-hook (cond-> {:timeout-ms hook-timeout-ms
                                :retries hook-retries
                                :retry-delay-ms 0}
                         state-dir
                         (assoc :cmd (hook-command state-dir)))}}
      remote?
      (assoc :datalevin/remote-fencing-retry? true))))
