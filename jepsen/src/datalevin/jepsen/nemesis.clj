(ns datalevin.jepsen.nemesis
  (:require
   [datalevin.jepsen.local :as local]
   [jepsen.generator :as gen]
   [jepsen.net.proto :as net.proto]
   [jepsen.nemesis :as n]))

(def ^:private default-failover-interval-s 10)
(def ^:private default-restart-delay-s 5)
(def ^:private default-pause-interval-s 10)
(def ^:private default-pause-resume-delay-s 5)
(def ^:private default-pause-leader-settle-timeout-ms 1000)
(def ^:private default-follower-rejoin-interval-s 10)
(def ^:private default-follower-rejoin-delay-s 5)
(def ^:private default-quorum-loss-interval-s 10)
(def ^:private default-quorum-restore-delay-s 5)
(def ^:private default-clock-skew-interval-s 10)
(def ^:private default-clock-skew-apply-delay-s 3)
(def ^:private default-partition-interval-s 10)
(def ^:private default-partition-heal-delay-s 5)
(def ^:private default-asymmetric-partition-interval-s 10)
(def ^:private default-asymmetric-heal-delay-s 5)
(def ^:private default-degraded-network-interval-s 10)
(def ^:private default-degraded-network-restore-delay-s 5)
(def ^:private default-degraded-network-profile
  {:delay-ms 250
   :jitter-ms 250
   :drop-probability 0.2})

(def supported-faults
  #{:leader-failover
    :leader-pause
    :leader-partition
    :asymmetric-partition
    :degraded-network
    :follower-rejoin
    :quorum-loss
    :clock-skew-pause})

(def ^:private alias-faults
  {:none     []
   :failover [:leader-failover]
   :pause    [:leader-pause]
   :partition [:leader-partition]
   :asymmetric [:asymmetric-partition]
   :degraded [:degraded-network]
   :rejoin   [:follower-rejoin]
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

(defn- maybe-wait-for-replacement-leader
  [cluster-id old-leader timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [last-state nil]
      (if-let [{leader :leader :as state}
               (local/maybe-wait-for-authority-leader cluster-id 1000)]
        (if (not= old-leader leader)
          state
          (if (< (System/currentTimeMillis) deadline)
            (do
              (Thread/sleep 250)
              (recur state))
            last-state))
        (when (< (System/currentTimeMillis) deadline)
          (Thread/sleep 250)
          (recur last-state))))))

(defn- partition-net!
  [test cluster-id grudge]
  (if-let [net (:net test)]
    (net.proto/drop-all! net test grudge)
    (local/apply-network-grudge! cluster-id grudge)))

(defn- heal-net!
  [test cluster-id]
  (if-let [net (:net test)]
    (net.proto/heal! net test)
    (local/heal-network! cluster-id)))

(defn- shape-net!
  [test cluster-id nodes behavior]
  (if-let [net (:net test)]
    (net.proto/shape! net test nodes behavior)
    (local/apply-network-shape! cluster-id nodes behavior)))

(defn- fast-net!
  [test cluster-id]
  (if-let [net (:net test)]
    (net.proto/fast! net test)
    (local/heal-network! cluster-id)))

(defn- leader-failover-nemesis
  []
  (let [stopped-node        (atom nil)
        paused-node         (atom nil)
        rejoin-stopped-node (atom nil)
        quorum-stopped-nodes (atom [])
        skewed-nodes         (atom [])
        active-partition     (atom nil)
        active-asymmetric-partition (atom nil)
        active-degraded-network (atom nil)]
    (reify
      n/Reflection
      (fs [_]
        #{:kill-leader :restart-node
          :pause-leader :resume-node
          :partition-leader :heal-partition
          :partition-asymmetric :heal-asymmetric
          :degrade-network :restore-network
          :stop-follower :restart-follower
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

              :pause-leader
              (if-let [node @paused-node]
                (info-op op {:paused node
                             :status :already-paused})
                (let [{:keys [leader]} (local/wait-for-single-leader!
                                        cluster-id)]
                  (local/pause-node! cluster-id leader)
                  (reset! paused-node leader)
                  (if-let [{new-leader :leader}
                           (local/maybe-wait-for-single-leader
                            cluster-id
                            default-pause-leader-settle-timeout-ms)]
                    (info-op op {:paused leader
                                 :leader new-leader})
                    (info-op op {:paused leader
                                 :leader nil
                                 :status :leader-unavailable}))))

              :resume-node
              (if-let [node @paused-node]
                (do
                  (local/resume-node! cluster-id node)
                  (reset! paused-node nil)
                  (if-let [{leader :leader}
                           (local/maybe-wait-for-single-leader cluster-id)]
                    (info-op op {:resumed node
                                 :leader leader})
                    (info-op op {:resumed node
                                 :leader nil
                                 :status :leader-unavailable})))
                (info-op op :noop))

              :partition-leader
              (if-let [{:keys [leader grudge]} @active-partition]
                (info-op op {:partitioned leader
                             :grudge grudge
                             :status :already-partitioned})
                (let [{:keys [leader]} (local/wait-for-authority-leader!
                                        cluster-id)
                      grudge (local/leader-partition-grudge cluster-id leader)]
                  (if (seq grudge)
                    (do
                      (partition-net! test cluster-id grudge)
                      (reset! active-partition {:leader leader
                                                :grudge grudge})
                      (if-let [{new-leader :leader}
                               (maybe-wait-for-replacement-leader
                                cluster-id
                                leader
                                30000)]
                        (info-op op {:partitioned leader
                                     :leader new-leader
                                     :grudge grudge})
                        (info-op op {:partitioned leader
                                     :leader leader
                                     :grudge grudge
                                     :status :leader-unchanged})))
                    (info-op op {:partitioned leader
                                 :grudge {}
                                 :status :no-follower-available}))))

              :heal-partition
              (if-let [{:keys [leader grudge]} @active-partition]
                (do
                  (heal-net! test cluster-id)
                  (reset! active-partition nil)
                  (if-let [{healed-leader :leader}
                           (local/maybe-wait-for-authority-leader cluster-id)]
                    (info-op op {:healed leader
                                 :leader healed-leader
                                 :grudge grudge})
                    (info-op op {:healed leader
                                 :leader nil
                                 :grudge grudge
                                 :status :leader-unavailable})))
                (info-op op :noop))

              :partition-asymmetric
              (if-let [{:keys [grudge]} @active-asymmetric-partition]
                (info-op op {:grudge grudge
                             :status :already-partitioned})
                (if-let [{:keys [left right direction grudge]}
                         (local/random-graph-cut cluster-id)]
                  (do
                    (partition-net! test cluster-id grudge)
                    (reset! active-asymmetric-partition
                            {:left left
                             :right right
                             :direction direction
                             :grudge grudge})
                    (if-let [{leader :leader}
                             (local/maybe-wait-for-authority-leader
                              cluster-id
                              30000)]
                      (info-op op {:left left
                                   :right right
                                   :direction direction
                                   :leader leader
                                   :grudge grudge})
                      (info-op op {:left left
                                   :right right
                                   :direction direction
                                   :leader nil
                                   :grudge grudge
                                   :status :leader-unavailable})))
                  (info-op op {:status :insufficient-live-nodes})))

              :heal-asymmetric
              (if-let [{:keys [left right direction grudge]}
                       @active-asymmetric-partition]
                (do
                  (heal-net! test cluster-id)
                  (reset! active-asymmetric-partition nil)
                  (if-let [{leader :leader}
                           (local/maybe-wait-for-authority-leader cluster-id)]
                    (info-op op {:left left
                                 :right right
                                 :direction direction
                                 :leader leader
                                 :grudge grudge})
                    (info-op op {:left left
                                 :right right
                                 :direction direction
                                 :leader nil
                                 :grudge grudge
                                 :status :leader-unavailable})))
                (info-op op :noop))

              :degrade-network
              (if-let [{:keys [nodes behavior]} @active-degraded-network]
                (info-op op {:nodes nodes
                             :behavior behavior
                             :status :already-degraded})
                (let [nodes (-> (local/cluster-state cluster-id)
                                :live-nodes
                                sort
                                vec)
                      behavior default-degraded-network-profile]
                  (if (> (count nodes) 1)
                    (do
                      (shape-net! test cluster-id nodes behavior)
                      (reset! active-degraded-network
                              {:nodes nodes
                               :behavior behavior})
                      (if-let [{leader :leader}
                               (local/maybe-wait-for-authority-leader
                                cluster-id
                                30000)]
                        (info-op op {:nodes nodes
                                     :leader leader
                                     :behavior behavior})
                        (info-op op {:nodes nodes
                                     :leader nil
                                     :behavior behavior
                                     :status :leader-unavailable})))
                    (info-op op {:nodes nodes
                                 :status :insufficient-live-nodes}))))

              :restore-network
              (if-let [{:keys [nodes behavior]} @active-degraded-network]
                (do
                  (fast-net! test cluster-id)
                  (reset! active-degraded-network nil)
                  (if-let [{leader :leader}
                           (local/maybe-wait-for-authority-leader cluster-id)]
                    (info-op op {:nodes nodes
                                 :leader leader
                                 :behavior behavior})
                    (info-op op {:nodes nodes
                                 :leader nil
                                 :behavior behavior
                                 :status :leader-unavailable})))
                (info-op op :noop))

              :stop-follower
              (if-let [node @rejoin-stopped-node]
                (info-op op {:stopped node
                             :status :already-stopped})
                (let [{:keys [leader]} (local/wait-for-single-leader!
                                        cluster-id)
                      {:keys [live-nodes]} (local/cluster-state cluster-id)
                      follower             (->> live-nodes
                                                sort
                                                (remove #{leader})
                                                first)]
                  (if follower
                    (do
                      (local/stop-node! cluster-id follower)
                      (reset! rejoin-stopped-node follower)
                      (info-op op {:stopped follower
                                   :leader leader}))
                    (info-op op {:stopped nil
                                 :leader leader
                                 :status :no-follower-available}))))

              :restart-follower
              (if-let [node @rejoin-stopped-node]
                (do
                  (local/restart-node! cluster-id node)
                  (reset! rejoin-stopped-node nil)
                  (if-let [{leader :leader}
                           (local/maybe-wait-for-single-leader cluster-id)]
                    (info-op op {:restarted node
                                 :leader leader})
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
           pause-interval-s
           pause-resume-delay-s
           partition-interval-s
           partition-heal-delay-s
           asymmetric-partition-interval-s
           asymmetric-heal-delay-s
           degraded-network-interval-s
           degraded-network-restore-delay-s
           follower-rejoin-interval-s
           follower-rejoin-delay-s
           quorum-loss-interval-s
           quorum-restore-delay-s
           clock-skew-interval-s
           clock-skew-apply-delay-s]}]
  (let [faults         (set faults)
        failover?      (contains? faults :leader-failover)
        pause?         (contains? faults :leader-pause)
        partition?     (contains? faults :leader-partition)
        asymmetric-partition? (contains? faults :asymmetric-partition)
        degraded-network? (contains? faults :degraded-network)
        follower-rejoin? (contains? faults :follower-rejoin)
        quorum-loss?   (contains? faults :quorum-loss)
        clock-skew?    (contains? faults :clock-skew-pause)
        restart-delay  (or restart-delay-s default-restart-delay-s)
        pause-resume-delay
        (or pause-resume-delay-s default-pause-resume-delay-s)
        partition-heal-delay
        (or partition-heal-delay-s default-partition-heal-delay-s)
        asymmetric-heal-delay
        (or asymmetric-heal-delay-s default-asymmetric-heal-delay-s)
        degraded-network-restore-delay
        (or degraded-network-restore-delay-s
            default-degraded-network-restore-delay-s)
        follower-rejoin-delay
        (or follower-rejoin-delay-s default-follower-rejoin-delay-s)
        failover-interval
        (or failover-interval-s default-failover-interval-s)
        pause-interval
        (or pause-interval-s default-pause-interval-s)
        partition-interval
        (or partition-interval-s default-partition-interval-s)
        asymmetric-partition-interval
        (or asymmetric-partition-interval-s
            default-asymmetric-partition-interval-s)
        degraded-network-interval
        (or degraded-network-interval-s default-degraded-network-interval-s)
        follower-rejoin-interval
        (or follower-rejoin-interval-s default-follower-rejoin-interval-s)
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
                           (when pause?
                             [{:type :info :f :pause-leader}
                              (gen/sleep pause-resume-delay)
                              {:type :info :f :resume-node}
                              (gen/sleep pause-interval)])
                           (when partition?
                             [{:type :info :f :partition-leader}
                              (gen/sleep partition-heal-delay)
                              {:type :info :f :heal-partition}
                              (gen/sleep partition-interval)])
                           (when asymmetric-partition?
                             [{:type :info :f :partition-asymmetric}
                              (gen/sleep asymmetric-heal-delay)
                              {:type :info :f :heal-asymmetric}
                              (gen/sleep asymmetric-partition-interval)])
                           (when degraded-network?
                             [{:type :info :f :degrade-network}
                              (gen/sleep degraded-network-restore-delay)
                              {:type :info :f :restore-network}
                              (gen/sleep degraded-network-interval)])
                           (when clock-skew?
                             [{:type :info :f :inject-clock-skew}
                              (gen/sleep clock-skew-apply-delay)
                              {:type :info :f :clear-clock-skew}
                              (gen/sleep clock-skew-interval)])))
                        (when follower-rejoin?
                          [{:type :info :f :stop-follower}
                           (gen/sleep follower-rejoin-delay)
                           {:type :info :f :restart-follower}
                           (gen/sleep follower-rejoin-interval)])
                        (when quorum-loss?
                          [{:type :info :f :lose-quorum}
                           (gen/sleep quorum-restore-delay)
                           {:type :info :f :restore-quorum}
                           (gen/sleep quorum-loss-interval)]))
        final-phases   (concat
                        (when failover?
                          [{:type :info :f :restart-node}])
                        (when pause?
                          [{:type :info :f :resume-node}])
                        (when partition?
                          [{:type :info :f :heal-partition}])
                        (when asymmetric-partition?
                          [{:type :info :f :heal-asymmetric}])
                        (when degraded-network?
                          [{:type :info :f :restore-network}])
                        (when follower-rejoin?
                          [{:type :info :f :restart-follower}])
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
