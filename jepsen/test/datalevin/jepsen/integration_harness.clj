(ns datalevin.jepsen.integration-harness
  (:require
   [datalevin.core :as d]
   [datalevin.jepsen.core :as core]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.workload.append :as append]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [jepsen.db :as jdb]
   [jepsen.history :as history]
   [jepsen.nemesis :as jn]))

(def ^:private local-history-start-time "20260312T000000.000-0800")
;; The production sofa-jraft control plane converges more slowly than the old
;; in-process test backend, so give failover smoke checks a wider replication
;; window before declaring live-node catch-up failed.
(def ^:private local-history-convergence-timeout-ms 60000)

(defn wait-for-leader-append-write!
  [test key value timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [last-error nil]
      (let [result (try
                     (local/with-leader-conn
                       test
                       append/schema
                       (fn [conn]
                         (d/transact! conn [{:append/key key
                                             :append/value value}])
                         :committed))
                     (catch Throwable e
                       e))]
        (cond
          (= :committed result)
          :committed

          (and (instance? Throwable result)
               (< (System/currentTimeMillis) deadline)
               (or (local/transport-failure? result)
                   (local/expected-disruption-write-failure? test result)))
          (do
            (Thread/sleep 250)
            (recur result))

          (instance? Throwable result)
          (throw result)

          :else
          (throw (ex-info "Timed out waiting for append write"
                          {:test (:db-name test)
                           :key key
                           :value value
                           :timeout-ms timeout-ms
                           :last-error (some-> last-error ex-message)})))))))

(defn write-append-batch!
  [test key values sleep-ms]
  (local/with-leader-conn
    test
    append/schema
    (fn [conn]
      (doseq [value values]
        (d/transact! conn [{:append/key   (long key)
                            :append/value (long value)}])
        (when (pos? (long sleep-ms))
          (Thread/sleep (long sleep-ms)))))))

(defn local-append-values
  [cluster-id logical-node key]
  (let [values (local/local-query
                 cluster-id
                 logical-node
                 '[:find [?v ...]
                   :in $ ?key
                   :where
                   [?e :append/key ?key]
                   [?e :append/value ?v]]
                 (long key))]
    (when-not (= ::local/unavailable values)
      (->> values
           (map long)
           sort
           vec))))

(defn wait-for-append-values-on-nodes!
  [cluster-id logical-nodes key expected-values timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))
        nodes    (vec logical-nodes)
        expected (->> expected-values
                      (map long)
                      vec)]
    (loop [last-values nil]
      (let [observed (into {}
                           (map (fn [logical-node]
                                  [logical-node
                                   (local-append-values cluster-id
                                                        logical-node
                                                        key)]))
                           nodes)]
        (if (every? (fn [[_ values]] (= expected values)) observed)
          observed
          (if (< (System/currentTimeMillis) deadline)
            (do
              (Thread/sleep 250)
              (recur observed))
            (throw (ex-info "Timed out waiting for append values on nodes"
                            {:cluster-id cluster-id
                             :logical-nodes nodes
                             :key key
                             :expected-values expected
                             :timeout-ms timeout-ms
                             :last-values last-values}))))))))

(defn- invoke-history-op!
  [opened-client test process-id op]
  (let [invoke-op     (assoc op
                             :type :invoke
                             :process process-id)
        completion-op (client/invoke! opened-client test invoke-op)]
    [invoke-op completion-op]))

(defn run-local-history-failover-check!
  [db-name workload-name workload-opts pre-ops during-fault-ops post-ops]
  (let [test-opts (merge {:db-name db-name
                          :control-backend :sofa-jraft
                          :workload workload-name
                          :rate 1
                          :time-limit 5
                          :nodes ["n1" "n2" "n3"]
                          :nemesis [:leader-failover]
                          :verbose false}
                         workload-opts)
        workload  ((get core/workloads workload-name) test-opts)
        checker   (or (:datalevin/history-checker test-opts)
                      (:checker workload))
        test-map  (assoc (core/datalevin-test test-opts)
                         :start-time local-history-start-time)
        db        (:db test-map)
        client    (:client test-map)
        nemesis   (:nemesis test-map)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened         (client/open! client test-map "n1")
            _              (client/setup! opened test-map)
            process-id     (atom -1)
            history-ops    (atom [])
            record-op!     (fn [op]
                             (let [[invoke-op completion-op]
                                   (invoke-history-op!
                                    opened
                                    test-map
                                    (swap! process-id inc)
                                    op)]
                               (swap! history-ops into [invoke-op completion-op])
                               completion-op))
            cluster-id     (:datalevin/cluster-id test-map)
            leader-before  (:leader (local/wait-for-single-leader!
                                     cluster-id
                                     60000))
            _              (doseq [op pre-ops]
                             (record-op! op))
            pre-fault-lsn  (local/node-progress-lsn cluster-id
                                                   leader-before)
            _              (when (pos? (long (or pre-fault-lsn 0)))
                             (local/wait-for-live-nodes-at-least-lsn!
                              cluster-id
                              pre-fault-lsn
                              local-history-convergence-timeout-ms))
            failover-op    (jn/invoke! nemesis
                                       test-map
                                       {:type :info
                                        :process :nemesis
                                        :f :kill-leader})
            _              (doseq [op during-fault-ops]
                             (record-op! op))
            stabilize-op   (jn/invoke! nemesis
                                       test-map
                                       {:type :info
                                        :process :nemesis
                                        :f :stabilize-leader})
            _              (doseq [op post-ops]
                             (record-op! op))
            leader-after   (:leader (local/wait-for-single-leader!
                                     cluster-id
                                     local-history-convergence-timeout-ms))
            _              (local/wait-for-live-nodes-at-least-lsn!
                            cluster-id
                            (local/node-progress-lsn cluster-id
                                                     leader-after)
                            local-history-convergence-timeout-ms)
            checker-result (checker/check checker
                                          test-map
                                          (history/history @history-ops)
                                          nil)]
        {:test-map test-map
         :history @history-ops
         :checker-result checker-result
         :leader-before leader-before
         :leader-after leader-after
         :failover-op failover-op
         :stabilize-op stabilize-op})
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))
