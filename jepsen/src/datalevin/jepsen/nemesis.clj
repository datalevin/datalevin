(ns datalevin.jepsen.nemesis
  (:require
   [datalevin.jepsen.local :as local]
   [jepsen.generator :as gen]
   [jepsen.net.proto :as net.proto]
   [jepsen.nemesis :as n]))

(def ^:private default-failover-interval-s 10)
(def ^:private default-kill-interval-s 10)
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
(def ^:private default-final-leader-stabilize-timeout-ms 30000)
(def ^:private default-partition-interval-s 10)
(def ^:private default-partition-heal-delay-s 5)
(def ^:private default-asymmetric-partition-interval-s 10)
(def ^:private default-asymmetric-heal-delay-s 5)
(def ^:private default-degraded-network-interval-s 10)
(def ^:private default-degraded-network-restore-delay-s 5)
(def ^:private default-io-stall-interval-s 10)
(def ^:private default-io-stall-heal-delay-s 5)
(def ^:private default-disk-full-interval-s 10)
(def ^:private default-disk-full-heal-delay-s 5)

(def supported-faults
  #{:leader-failover
    :node-kill
    :leader-pause
    :node-pause
    :multi-node-pause
    :leader-partition
    :asymmetric-partition
    :degraded-network
    :leader-io-stall
    :leader-disk-full
    :follower-rejoin
    :quorum-loss
    :clock-skew-pause
    :clock-skew-leader-fast
    :clock-skew-leader-slow
    :clock-skew-mixed})

(def ^:private alias-faults
  {:none []
   :failover [:leader-failover]
   :kill [:node-kill]
   :pause [:leader-pause]
   :pause-any [:node-pause]
   :pause-multi [:multi-node-pause]
   :partition [:leader-partition]
   :asymmetric [:asymmetric-partition]
   :degraded [:degraded-network]
   :io-stall [:leader-io-stall]
   :disk-full [:leader-disk-full]
   :rejoin [:follower-rejoin]
   :quorum [:quorum-loss]
   :clock-skew [:clock-skew-pause]
   :clock-leader-fast [:clock-skew-leader-fast]
   :clock-leader-slow [:clock-skew-leader-slow]
   :clock-mixed [:clock-skew-mixed]})

(def ^:private legacy-clock-skew-patterns
  [:followers-fast :leader-fast :leader-slow :mixed])

(def ^:private explicit-clock-skew-patterns
  {:clock-skew-leader-fast :leader-fast
   :clock-skew-leader-slow :leader-slow
   :clock-skew-mixed :mixed})

(def ^:private explicit-clock-skew-fault-order
  [:clock-skew-leader-fast
   :clock-skew-leader-slow
   :clock-skew-mixed])

(defn clock-skew-fault?
  [fault]
  (or (= :clock-skew-pause fault)
      (contains? explicit-clock-skew-patterns fault)))

(defn- active-clock-skew-patterns
  [faults]
  (let [faults (set faults)]
    (->> (concat
          (when (contains? faults :clock-skew-pause)
            legacy-clock-skew-patterns)
          (map explicit-clock-skew-patterns
               (filter faults explicit-clock-skew-fault-order)))
       distinct
       vec)))

(defn- clock-skew-plan
  [{:keys [leader live-nodes budget-ms pattern]}]
  (let [live-nodes  (->> live-nodes sort vec)
        followers   (vec (remove #{leader} live-nodes))
        skew-ms     (long (max 250 (* 2 (long budget-ms))))
        slow-skew   (- skew-ms)
        mixed-skews (into {leader skew-ms}
                          (map-indexed
                           (fn [idx node]
                             [node (if (even? idx) slow-skew skew-ms)]))
                          followers)]
    {:leader leader
     :pattern pattern
     :clock-skew-ms skew-ms
     :skews (case pattern
              :followers-fast
              (zipmap followers (repeat skew-ms))

              :leader-fast
              (into {leader skew-ms}
                    (map (fn [node] [node slow-skew]))
                    followers)

              :leader-slow
              (into {leader slow-skew}
                    (map (fn [node] [node skew-ms]))
                    followers)

              :mixed
              mixed-skews)}))

(defn- clock-skew-phase-ops
  [patterns apply-delay interval]
  (mapcat (fn [pattern]
            [{:type :info
              :f :inject-clock-skew
              :value {:pattern pattern}}
             (gen/sleep apply-delay)
             {:type :info :f :clear-clock-skew}
             (gen/sleep interval)])
          patterns))

(defn- clock-skew-failover-phase-ops
  [patterns apply-delay restart-delay failover-interval]
  (mapcat (fn [pattern]
            [{:type :info
              :f :inject-clock-skew
              :value {:pattern pattern}}
             (gen/sleep apply-delay)
             {:type :info :f :kill-leader}
             (gen/sleep restart-delay)
             {:type :info :f :restart-node}
             {:type :info :f :clear-clock-skew}
             (gen/sleep failover-interval)])
          patterns))

(defn- final-phase-ops
  [{:keys [failover?
           kill?
           leader-pause?
           node-pause?
           multi-node-pause?
           partition?
           asymmetric-partition?
           degraded-network?
           io-stall?
           disk-full?
           follower-rejoin?
           quorum-loss?
           clock-skew?]}]
  (let [clock-skew-failover? (and failover? clock-skew?)]
    (concat
     (when clock-skew-failover?
       [{:type :info :f :clear-clock-skew}
        {:type :info :f :restart-node}
        {:type :info :f :stabilize-leader}])
     (when (and failover? (not clock-skew-failover?))
       [{:type :info :f :restart-node}])
     (when kill?
       [{:type :info :f :restart-killed-node}])
     (when (or leader-pause?
               node-pause?
               multi-node-pause?)
       [{:type :info :f :resume-node}])
     (when partition?
       [{:type :info :f :heal-partition}])
     (when asymmetric-partition?
       [{:type :info :f :heal-asymmetric}])
     (when degraded-network?
       [{:type :info :f :restore-network}])
     (when (or io-stall? disk-full?)
       [{:type :info :f :heal-storage}])
     (when follower-rejoin?
       [{:type :info :f :restart-follower}])
     (when quorum-loss?
       [{:type :info :f :restore-quorum}])
     (when (and clock-skew? (not clock-skew-failover?))
       [{:type :info :f :clear-clock-skew}]))))

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

(defn- available-pause-nodes
  [cluster-id paused-nodes]
  (let [{:keys [live-nodes]} (local/cluster-state cluster-id)]
    (->> live-nodes
         sort
         (remove paused-nodes)
         vec)))

(defn- pick-node-to-pause
  [cluster-id paused-nodes]
  (when-let [candidates (seq (available-pause-nodes cluster-id paused-nodes))]
    (rand-nth (vec candidates))))

(defn- pick-nodes-to-pause
  [cluster-id paused-nodes requested-count]
  (let [candidates (available-pause-nodes cluster-id paused-nodes)
        candidate-count (count candidates)
        max-pause-count (max 0 (dec candidate-count))]
    (when (pos? max-pause-count)
      (let [pause-count (-> (or requested-count
                                (inc (rand-int max-pause-count)))
                            long
                            (max 1)
                            (min max-pause-count))]
        (->> candidates
             shuffle
             (take pause-count)
             vec)))))

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
  (let [stopped-node (atom nil)
        killed-node (atom nil)
        paused-nodes (atom #{})
        rejoin-stopped-node (atom nil)
        quorum-stopped-nodes (atom [])
        active-clock-skew (atom nil)
        active-partition (atom nil)
        active-asymmetric-partition (atom nil)
        active-degraded-network (atom nil)
        active-storage-fault (atom nil)]
    (reify
      n/Reflection
      (fs [_]
        #{:kill-leader :restart-node :stabilize-leader
          :kill-node :restart-killed-node
          :pause-leader :pause-node :pause-nodes
          :resume-node :resume-nodes
          :partition-leader :heal-partition
          :partition-asymmetric :heal-asymmetric
          :degrade-network :restore-network
          :wedge-leader-storage :heal-storage
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
                (if @active-clock-skew
                  (info-op op {:stopped leader
                               :leader nil
                               :status :leader-paused})
                  (if-let [{new-leader :leader}
                           (local/maybe-wait-for-single-leader cluster-id)]
                    (info-op op {:stopped leader
                                 :leader new-leader})
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
                            (if @active-clock-skew 1000 10000))]
                    (info-op op {:restarted node
                                 :leader leader})
                    (info-op op {:restarted node
                                 :leader nil
                                 :status :leader-unavailable})))
                (info-op op :noop))

              :kill-node
              (if-let [node @killed-node]
                (info-op op {:killed node
                             :status :already-stopped})
                (let [{:keys [live-nodes]} (local/cluster-state cluster-id)
                      candidate (or (get-in op [:value :node])
                                    (when (seq live-nodes)
                                      (rand-nth (vec (sort live-nodes)))))]
                  (if candidate
                    (if (contains? (set live-nodes) candidate)
                      (do
                        (local/stop-node! cluster-id candidate)
                        (reset! killed-node candidate)
                        (if-let [{leader :leader}
                                 (local/maybe-wait-for-single-leader
                                  cluster-id
                                  10000)]
                          (info-op op {:killed candidate
                                       :leader leader})
                          (info-op op {:killed candidate
                                       :leader nil
                                       :status :leader-unavailable})))
                      (info-op op {:killed candidate
                                   :status :invalid-node}))
                    (info-op op {:killed nil
                                 :status :insufficient-live-nodes}))))

              :restart-killed-node
              (if-let [node @killed-node]
                (do
                  (local/restart-node! cluster-id node)
                  (reset! killed-node nil)
                  (if-let [{leader :leader}
                           (local/maybe-wait-for-single-leader cluster-id)]
                    (info-op op {:restarted node
                                 :leader leader})
                    (info-op op {:restarted node
                                 :leader nil
                                 :status :leader-unavailable})))
                (info-op op :noop))

              :stabilize-leader
              (if-let [{leader :leader}
                       (local/maybe-wait-for-single-leader
                        cluster-id
                        default-final-leader-stabilize-timeout-ms)]
                (info-op op {:leader leader})
                (info-op op {:leader nil
                             :status :leader-unavailable}))

              :pause-leader
              (if (seq @paused-nodes)
                (info-op op {:paused-nodes (sort @paused-nodes)
                             :status :already-paused})
                (let [{:keys [leader]} (local/wait-for-single-leader!
                                        cluster-id)]
                  (local/pause-node! cluster-id leader)
                  (swap! paused-nodes conj leader)
                  (if-let [{new-leader :leader}
                           (local/maybe-wait-for-single-leader
                            cluster-id
                            default-pause-leader-settle-timeout-ms)]
                    (info-op op {:paused leader
                                 :leader new-leader})
                    (info-op op {:paused leader
                                 :leader nil
                                 :status :leader-unavailable}))))

              :pause-node
              (if (seq @paused-nodes)
                (info-op op {:paused-nodes (sort @paused-nodes)
                             :status :already-paused})
                (if-let [node (or (get-in op [:value :node])
                                  (pick-node-to-pause cluster-id @paused-nodes))]
                  (do
                    (if (some #{node}
                              (available-pause-nodes cluster-id @paused-nodes))
                      (do
                        (local/pause-node! cluster-id node)
                        (swap! paused-nodes conj node)
                        (if-let [{leader :leader}
                                 (local/maybe-wait-for-single-leader
                                  cluster-id
                                  default-pause-leader-settle-timeout-ms)]
                          (info-op op {:paused node
                                       :leader leader})
                          (info-op op {:paused node
                                       :leader nil
                                       :status :leader-unavailable})))
                      (info-op op {:paused node
                                   :status :invalid-node})))
                  (info-op op {:paused nil
                               :status :insufficient-live-nodes})))

              :pause-nodes
              (if (seq @paused-nodes)
                (info-op op {:paused-nodes (sort @paused-nodes)
                             :status :already-paused})
                (let [requested-nodes (some-> (get-in op [:value :nodes]) vec)
                      candidates (available-pause-nodes cluster-id
                                                        @paused-nodes)
                      nodes (or requested-nodes
                                (pick-nodes-to-pause
                                 cluster-id
                                 @paused-nodes
                                 (get-in op [:value :count])))
                      valid-request? (and (seq nodes)
                                          (< (count nodes) (count candidates))
                                          (every? (set candidates) nodes))]
                  (if (seq nodes)
                    (if valid-request?
                      (do
                        (doseq [node nodes]
                          (local/pause-node! cluster-id node))
                        (swap! paused-nodes into nodes)
                        (if-let [{leader :leader}
                                 (local/maybe-wait-for-single-leader
                                  cluster-id
                                  default-pause-leader-settle-timeout-ms)]
                          (info-op op {:paused-nodes (vec nodes)
                                       :leader leader})
                          (info-op op {:paused-nodes (vec nodes)
                                       :leader nil
                                       :status :leader-unavailable})))
                      (info-op op {:paused-nodes (vec nodes)
                                   :status :invalid-node-set}))
                    (info-op op {:paused-nodes []
                                 :status :insufficient-live-nodes}))))

              (:resume-node :resume-nodes)
              (if (seq @paused-nodes)
                (let [nodes (-> @paused-nodes sort vec)]
                  (doseq [node nodes]
                    (local/resume-node! cluster-id node))
                  (reset! paused-nodes #{})
                  (let [resume-value (if (and (= :resume-node (:f op))
                                              (= 1 (count nodes)))
                                       {:resumed (first nodes)}
                                       {:resumed-nodes nodes})]
                    (if-let [{leader :leader}
                             (local/maybe-wait-for-single-leader cluster-id)]
                      (info-op op (assoc resume-value :leader leader))
                      (info-op op (assoc resume-value
                                         :leader nil
                                         :status :leader-unavailable)))))
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
              (if-let [{:keys [grudge] :as cut} @active-asymmetric-partition]
                (info-op op (assoc cut
                                   :status :already-partitioned))
                (if-let [{:keys [grudge] :as cut}
                         (local/random-graph-cut cluster-id)]
                  (do
                    (partition-net! test cluster-id grudge)
                    (reset! active-asymmetric-partition cut)
                    (if-let [{leader :leader}
                             (local/maybe-wait-for-authority-leader
                              cluster-id
                              30000)]
                      (info-op op (assoc cut
                                         :leader leader))
                      (info-op op (assoc cut
                                         :leader nil
                                         :status :leader-unavailable))))
                  (info-op op {:status :insufficient-live-nodes})))

              :heal-asymmetric
              (if-let [{:keys [grudge] :as cut}
                       @active-asymmetric-partition]
                (do
                  (heal-net! test cluster-id)
                  (reset! active-asymmetric-partition nil)
                  (if-let [{leader :leader}
                           (local/maybe-wait-for-authority-leader cluster-id)]
                    (info-op op (assoc cut
                                       :leader leader))
                    (info-op op (assoc cut
                                       :leader nil
                                       :status :leader-unavailable))))
                (info-op op :noop))

              :degrade-network
              (if-let [{:keys [nodes] :as behavior} @active-degraded-network]
                (info-op op {:nodes nodes
                             :behavior behavior
                             :status :already-degraded})
                (if-let [{:keys [nodes] :as behavior}
                         (local/random-degraded-network-shape cluster-id)]
                  (if (> (count nodes) 1)
                    (do
                      (shape-net! test cluster-id nodes behavior)
                      (reset! active-degraded-network behavior)
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
                                 :status :insufficient-live-nodes}))
                  (info-op op {:status :insufficient-live-nodes})))

              :restore-network
              (if-let [{:keys [nodes] :as behavior} @active-degraded-network]
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

              :wedge-leader-storage
              (if-let [{:keys [leader fault]} @active-storage-fault]
                (info-op op {:wedged leader
                             :fault fault
                             :status :already-wedged})
                (let [mode (keyword (or (get-in op [:value :mode]) :stall))
                      {leader :leader} (local/wait-for-authority-leader!
                                        cluster-id)
                      fault (local/wedge-node-storage! cluster-id
                                                       leader
                                                       {:mode mode})]
                  (reset! active-storage-fault {:leader leader
                                                :fault fault})
                  (info-op op {:wedged leader
                               :fault fault
                               :leader (:leader
                                        (local/maybe-wait-for-authority-leader
                                         cluster-id
                                         1000))})))

              :heal-storage
              (if-let [{:keys [leader fault]} @active-storage-fault]
                (let [cleared (local/heal-node-storage! cluster-id leader)]
                  (reset! active-storage-fault nil)
                  (info-op op {:healed leader
                               :fault (or cleared fault)
                               :leader (:leader
                                        (local/maybe-wait-for-authority-leader
                                         cluster-id
                                         1000))}))
                (info-op op :noop))

              :stop-follower
              (if-let [node @rejoin-stopped-node]
                (info-op op {:stopped node
                             :status :already-stopped})
                (let [{:keys [leader]} (local/wait-for-single-leader!
                                        cluster-id)
                      {:keys [live-nodes]} (local/cluster-state cluster-id)
                      follower (->> live-nodes
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
                             :status :already-lost})
                (let [{:keys [nodes live-nodes]} (local/cluster-state cluster-id)
                      total-nodes (count nodes)
                      quorum-size (inc (quot total-nodes 2))
                      max-live-nodes (dec quorum-size)
                      stop-needed (max 0 (- (count live-nodes)
                                            max-live-nodes))]
                  (if (zero? stop-needed)
                    (info-op op {:stopped []
                                 :status :already-lost})
                    (let [{:keys [leader]} (local/wait-for-single-leader!
                                            cluster-id)
                          ordered-live (->> live-nodes sort vec)
                          followers (remove #{leader} ordered-live)
                          nodes-to-stop (vec (take stop-needed
                                                   (concat followers
                                                           [leader])))]
                      (doseq [node nodes-to-stop]
                        (local/stop-node! cluster-id node))
                      (reset! quorum-stopped-nodes nodes-to-stop)
                      (info-op op {:stopped nodes-to-stop
                                   :leader leader})))))

              :restore-quorum
              (if (seq @quorum-stopped-nodes)
                (let [nodes-to-restart @quorum-stopped-nodes]
                  (doseq [node nodes-to-restart]
                    (local/restart-node! cluster-id node))
                  (reset! quorum-stopped-nodes [])
                  (if-let [{leader :leader}
                           (local/maybe-wait-for-single-leader cluster-id)]
                    (info-op op {:restarted nodes-to-restart
                                 :leader leader})
                    (info-op op {:restarted nodes-to-restart
                                 :leader nil
                                 :status :leader-unavailable})))
                (info-op op :noop))

              :inject-clock-skew
              (if @active-clock-skew
                (info-op op (assoc @active-clock-skew
                              :skewed-nodes (vec (keys (:skews @active-clock-skew)))
                              :status :already-skewed))
                (if-not (local/clock-skew-enabled? cluster-id)
                  (info-op-error op :clock-skew-not-configured)
                  (let [{:keys [leader]} (local/wait-for-single-leader!
                                          cluster-id)
                        live-nodes (-> (local/cluster-state cluster-id)
                                       :live-nodes
                                       sort
                                       vec)
                        budget-ms (local/clock-skew-budget-ms cluster-id)
                        pattern (or (get-in op [:value :pattern])
                                    (rand-nth legacy-clock-skew-patterns))
                        plan (clock-skew-plan {:leader leader
                                               :live-nodes live-nodes
                                               :budget-ms budget-ms
                                               :pattern pattern})]
                    (doseq [[node skew-ms] (:skews plan)]
                      (local/set-node-clock-skew! cluster-id node skew-ms))
                    (reset! active-clock-skew plan)
                    (info-op op (assoc plan
                                  :skewed-nodes (vec (keys (:skews plan))))))))

              :clear-clock-skew
              (if-let [plan @active-clock-skew]
                (let [nodes-to-clear (vec (keys (:skews plan)))]
                  (doseq [node nodes-to-clear]
                    (local/set-node-clock-skew! cluster-id node 0))
                  (reset! active-clock-skew nil)
                  (info-op op {:cleared-nodes nodes-to-clear
                               :pattern (:pattern plan)}))
                (info-op op :noop))

              (info-op-error op [:unsupported-nemesis-op (:f op)]))
            (catch Throwable e
              (info-op-error op (error-value e))))))

      (teardown! [_ _test]
        nil))))

(defn nemesis-package
  [{:keys [faults
           failover-interval-s
           kill-interval-s
           restart-delay-s
           pause-interval-s
           pause-resume-delay-s
           partition-interval-s
           partition-heal-delay-s
           asymmetric-partition-interval-s
           asymmetric-heal-delay-s
           degraded-network-interval-s
           degraded-network-restore-delay-s
           io-stall-interval-s
           io-stall-heal-delay-s
           disk-full-interval-s
           disk-full-heal-delay-s
           follower-rejoin-interval-s
           follower-rejoin-delay-s
           quorum-loss-interval-s
           quorum-restore-delay-s
           clock-skew-interval-s
           clock-skew-apply-delay-s]}]
  (let [faults (set faults)
        failover? (contains? faults :leader-failover)
        kill? (contains? faults :node-kill)
        leader-pause? (contains? faults :leader-pause)
        node-pause? (contains? faults :node-pause)
        multi-node-pause? (contains? faults :multi-node-pause)
        partition? (contains? faults :leader-partition)
        asymmetric-partition? (contains? faults :asymmetric-partition)
        degraded-network? (contains? faults :degraded-network)
        io-stall? (contains? faults :leader-io-stall)
        disk-full? (contains? faults :leader-disk-full)
        follower-rejoin? (contains? faults :follower-rejoin)
        quorum-loss? (contains? faults :quorum-loss)
        clock-skew-patterns (active-clock-skew-patterns faults)
        clock-skew? (seq clock-skew-patterns)
        restart-delay (or restart-delay-s default-restart-delay-s)
        pause-resume-delay
        (or pause-resume-delay-s default-pause-resume-delay-s)
        partition-heal-delay
        (or partition-heal-delay-s default-partition-heal-delay-s)
        asymmetric-heal-delay
        (or asymmetric-heal-delay-s default-asymmetric-heal-delay-s)
        degraded-network-restore-delay
        (or degraded-network-restore-delay-s
            default-degraded-network-restore-delay-s)
        io-stall-heal-delay
        (or io-stall-heal-delay-s default-io-stall-heal-delay-s)
        disk-full-heal-delay
        (or disk-full-heal-delay-s default-disk-full-heal-delay-s)
        follower-rejoin-delay
        (or follower-rejoin-delay-s default-follower-rejoin-delay-s)
        failover-interval
        (or failover-interval-s default-failover-interval-s)
        kill-interval
        (or kill-interval-s default-kill-interval-s)
        pause-interval
        (or pause-interval-s default-pause-interval-s)
        partition-interval
        (or partition-interval-s default-partition-interval-s)
        asymmetric-partition-interval
        (or asymmetric-partition-interval-s
            default-asymmetric-partition-interval-s)
        degraded-network-interval
        (or degraded-network-interval-s default-degraded-network-interval-s)
        io-stall-interval
        (or io-stall-interval-s default-io-stall-interval-s)
        disk-full-interval
        (or disk-full-interval-s default-disk-full-interval-s)
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
        phases (concat
                (if (and clock-skew? failover?)
                  (clock-skew-failover-phase-ops
                   clock-skew-patterns
                   clock-skew-apply-delay
                   restart-delay
                   failover-interval)
                  (concat
                   (when failover?
                     [{:type :info :f :kill-leader}
                      (gen/sleep restart-delay)
                      {:type :info :f :restart-node}
                      (gen/sleep failover-interval)])
                   (when kill?
                     [{:type :info :f :kill-node}
                      (gen/sleep restart-delay)
                      {:type :info :f :restart-killed-node}
                      (gen/sleep kill-interval)])
                   (when leader-pause?
                     [{:type :info :f :pause-leader}
                      (gen/sleep pause-resume-delay)
                      {:type :info :f :resume-node}
                      (gen/sleep pause-interval)])
                   (when node-pause?
                     [{:type :info :f :pause-node}
                      (gen/sleep pause-resume-delay)
                      {:type :info :f :resume-nodes}
                      (gen/sleep pause-interval)])
                   (when multi-node-pause?
                     [{:type :info :f :pause-nodes}
                      (gen/sleep pause-resume-delay)
                      {:type :info :f :resume-nodes}
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
                   (when io-stall?
                     [{:type :info
                       :f :wedge-leader-storage
                       :value {:mode :stall}}
                      (gen/sleep io-stall-heal-delay)
                      {:type :info :f :heal-storage}
                      (gen/sleep io-stall-interval)])
                   (when disk-full?
                     [{:type :info
                       :f :wedge-leader-storage
                       :value {:mode :disk-full}}
                      (gen/sleep disk-full-heal-delay)
                      {:type :info :f :heal-storage}
                      (gen/sleep disk-full-interval)])
                   (when clock-skew?
                     (clock-skew-phase-ops
                      clock-skew-patterns
                      clock-skew-apply-delay
                      clock-skew-interval))))
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
        final-phases (final-phase-ops
                      {:failover? failover?
                       :kill? kill?
                       :leader-pause? leader-pause?
                       :node-pause? node-pause?
                       :multi-node-pause? multi-node-pause?
                       :partition? partition?
                       :asymmetric-partition? asymmetric-partition?
                       :degraded-network? degraded-network?
                       :io-stall? io-stall?
                       :disk-full? disk-full?
                       :follower-rejoin? follower-rejoin?
                       :quorum-loss? quorum-loss?
                       :clock-skew? clock-skew?})
        needed? (seq phases)]
    {:generator (when needed?
                  (gen/cycle (apply gen/phases phases)))
     :final-generator (when (seq final-phases)
                        (apply gen/phases final-phases))
     :nemesis (if needed?
                (leader-failover-nemesis)
                n/noop)}))
