(ns datalevin.jepsen.nemesis
  (:require
   [datalevin.jepsen.local :as local]
   [jepsen.generator :as gen]
   [jepsen.nemesis :as n]))

(def ^:private default-failover-interval-s 10)
(def ^:private default-restart-delay-s 5)
(def ^:private default-quorum-loss-interval-s 10)
(def ^:private default-quorum-restore-delay-s 5)
(def ^:private default-clock-skew-interval-s 10)
(def ^:private default-clock-skew-apply-delay-s 3)

(def supported-faults
  #{:leader-failover
    :quorum-loss
    :clock-skew-pause})

(def ^:private alias-faults
  {:none     []
   :failover [:leader-failover]
   :quorum   [:quorum-loss]
   :clock-skew [:clock-skew-pause]})

(defn expand-fault
  [fault]
  (get alias-faults fault [fault]))

(defn- error-value
  [e]
  (or (ex-message e)
      (.getName (class e))))

(defn- info-op
  [op value]
  (assoc op :value value))

(defn- info-op-error
  [op error]
  (info-op op {:status :error
               :error error}))

(defn- leader-failover-nemesis
  []
  (let [stopped-node        (atom nil)
        quorum-stopped-nodes (atom [])
        skewed-nodes         (atom [])]
    (reify
      n/Reflection
      (fs [_]
        #{:kill-leader :restart-node
          :lose-quorum :restore-quorum
          :inject-clock-skew :clear-clock-skew})

      n/Nemesis
      (setup! [this _test]
        this)

      (invoke! [_ test op]
        (let [cluster-id (:datalevin/cluster-id test)]
          (try
            (case (:f op)
              :kill-leader
              (let [{:keys [leader]} (local/wait-for-single-leader! cluster-id)]
                (local/stop-node! cluster-id leader)
                (reset! stopped-node leader)
                (if (seq @skewed-nodes)
                  (info-op op {:stopped leader
                               :leader nil
                               :status :leader-paused})
                  (if-let [{new-leader :leader}
                           (local/maybe-wait-for-single-leader cluster-id)]
                    (info-op op {:stopped leader
                                 :leader  new-leader})
                    (info-op op {:stopped leader
                                 :leader nil
                                 :status :leader-unavailable}))))

              :restart-node
              (if-let [node @stopped-node]
                (do
                  (local/restart-node! cluster-id node)
                  (reset! stopped-node nil)
                  (if-let [{leader :leader}
                           (local/maybe-wait-for-single-leader
                            cluster-id
                            (if (seq @skewed-nodes) 1000 10000))]
                    (info-op op {:restarted node
                                 :leader    leader})
                    (info-op op {:restarted node
                                 :leader nil
                                 :status :leader-unavailable})))
                (info-op op :noop))

              :lose-quorum
              (if (seq @quorum-stopped-nodes)
                (info-op op {:stopped @quorum-stopped-nodes
                             :status  :already-lost})
                (let [{:keys [nodes live-nodes]} (local/cluster-state cluster-id)
                      total-nodes    (count nodes)
                      quorum-size    (inc (quot total-nodes 2))
                      max-live-nodes (dec quorum-size)
                      stop-needed    (max 0 (- (count live-nodes)
                                               max-live-nodes))]
                  (if (zero? stop-needed)
                    (info-op op {:stopped []
                                 :status  :already-lost})
                    (let [{:keys [leader]} (local/wait-for-single-leader!
                                            cluster-id)
                          ordered-live  (->> live-nodes sort vec)
                          followers     (remove #{leader} ordered-live)
                          nodes-to-stop (vec (take stop-needed
                                                   (concat followers
                                                           [leader])))]
                      (doseq [node nodes-to-stop]
                        (local/stop-node! cluster-id node))
                      (reset! quorum-stopped-nodes nodes-to-stop)
                      (info-op op {:stopped nodes-to-stop
                                   :leader  leader})))))

              :restore-quorum
              (if (seq @quorum-stopped-nodes)
                (let [nodes-to-restart @quorum-stopped-nodes]
                  (doseq [node nodes-to-restart]
                    (local/restart-node! cluster-id node))
                  (reset! quorum-stopped-nodes [])
                  (if-let [{leader :leader}
                           (local/maybe-wait-for-single-leader cluster-id)]
                    (info-op op {:restarted nodes-to-restart
                                 :leader    leader})
                    (info-op op {:restarted nodes-to-restart
                                 :leader nil
                                 :status :leader-unavailable})))
                (info-op op :noop))

              :inject-clock-skew
              (if (seq @skewed-nodes)
                (info-op op {:skewed-nodes @skewed-nodes
                             :status       :already-skewed})
                (if-not (local/clock-skew-enabled? cluster-id)
                  (info-op-error op :clock-skew-not-configured)
                  (let [{:keys [leader]} (local/wait-for-single-leader!
                                          cluster-id)
                        live-nodes    (-> (local/cluster-state cluster-id)
                                          :live-nodes
                                          sort
                                          vec)
                        nodes-to-skew (vec (remove #{leader} live-nodes))
                        budget-ms     (local/clock-skew-budget-ms cluster-id)
                        skew-ms       (long (max 250 (* 2 budget-ms)))]
                    (doseq [node nodes-to-skew]
                      (local/set-node-clock-skew! cluster-id node skew-ms))
                    (reset! skewed-nodes nodes-to-skew)
                    (info-op op {:leader        leader
                                 :skewed-nodes  nodes-to-skew
                                 :clock-skew-ms skew-ms}))))

              :clear-clock-skew
              (if (seq @skewed-nodes)
                (let [nodes-to-clear @skewed-nodes]
                  (doseq [node nodes-to-clear]
                    (local/set-node-clock-skew! cluster-id node 0))
                  (reset! skewed-nodes [])
                  (info-op op {:cleared-nodes nodes-to-clear}))
                (info-op op :noop))

              (info-op-error op [:unsupported-nemesis-op (:f op)]))
            (catch Throwable e
              (info-op-error op (error-value e))))))

      (teardown! [_ _test]
        nil))))

(defn nemesis-package
  [{:keys [faults
           failover-interval-s
           restart-delay-s
           quorum-loss-interval-s
           quorum-restore-delay-s
           clock-skew-interval-s
           clock-skew-apply-delay-s]}]
  (let [faults         (set faults)
        failover?      (contains? faults :leader-failover)
        quorum-loss?   (contains? faults :quorum-loss)
        clock-skew?    (contains? faults :clock-skew-pause)
        restart-delay  (or restart-delay-s default-restart-delay-s)
        failover-interval
        (or failover-interval-s default-failover-interval-s)
        quorum-restore-delay
        (or quorum-restore-delay-s default-quorum-restore-delay-s)
        quorum-loss-interval
        (or quorum-loss-interval-s default-quorum-loss-interval-s)
        clock-skew-apply-delay
        (or clock-skew-apply-delay-s default-clock-skew-apply-delay-s)
        clock-skew-interval
        (or clock-skew-interval-s default-clock-skew-interval-s)
        phases         (concat
                        (if (and clock-skew? failover?)
                          [{:type :info :f :inject-clock-skew}
                           (gen/sleep clock-skew-apply-delay)
                           {:type :info :f :kill-leader}
                           (gen/sleep restart-delay)
                           {:type :info :f :restart-node}
                           {:type :info :f :clear-clock-skew}
                           (gen/sleep failover-interval)]
                          (concat
                           (when failover?
                             [{:type :info :f :kill-leader}
                              (gen/sleep restart-delay)
                              {:type :info :f :restart-node}
                              (gen/sleep failover-interval)])
                           (when clock-skew?
                             [{:type :info :f :inject-clock-skew}
                              (gen/sleep clock-skew-apply-delay)
                              {:type :info :f :clear-clock-skew}
                              (gen/sleep clock-skew-interval)])))
                        (when quorum-loss?
                          [{:type :info :f :lose-quorum}
                           (gen/sleep quorum-restore-delay)
                           {:type :info :f :restore-quorum}
                           (gen/sleep quorum-loss-interval)]))
        final-phases   (concat
                        (when failover?
                          [{:type :info :f :restart-node}])
                        (when quorum-loss?
                          [{:type :info :f :restore-quorum}])
                        (when clock-skew?
                          [{:type :info :f :clear-clock-skew}]))
        needed?        (seq phases)]
    {:generator       (when needed?
                        (gen/cycle (apply gen/phases phases)))
     :final-generator (when (seq final-phases)
                        (apply gen/phases final-phases))
     :nemesis         (if needed?
                        (leader-failover-nemesis)
                        n/noop)}))
