(ns datalevin.jepsen.local.faults
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [datalevin.jepsen.remote :as remote]
   [datalevin.util :as u]
   [jepsen.net.proto :as net.proto])
  (:import
   [datalevin.jepsen PartitionFaults]
   [java.net ConnectException]
   [java.nio.channels ClosedChannelException]))

(def ^:private default-slow-link-profile
  {:delay-ms 250
   :jitter-ms 250
   :drop-probability 0.0})

(def ^:private default-flaky-link-profile
  {:delay-ms 0
   :jitter-ms 0
   :drop-probability 0.3})

(def ^:private degraded-network-profile-templates
  [{:delay-ms 25
    :jitter-ms 10
    :drop-probability 0.02}
   {:delay-ms 100
    :jitter-ms 25
    :drop-probability 0.05}
   {:delay-ms 250
    :jitter-ms 100
    :drop-probability 0.1}
   {:delay-ms 500
    :jitter-ms 200
    :drop-probability 0.2}
   {:delay-ms 750
    :jitter-ms 300
    :drop-probability 0.35}])

(def ^:private graph-cut-direction-modes
  [:none :left->right :right->left :bidirectional])

(def ^:private storage-fault-modes
  #{:stall :disk-full})

(def ^:private storage-fault-default-stages
  #{:txlog-append
    :txlog-sync
    :txlog-replay
    :txlog-force-sync
    :lmdb-sync})

(def ^:private storage-stall-poll-ms 100)
(def ^:private snapshot-unavailable-error-code
  :ha/follower-snapshot-unavailable)
(def ^:private snapshot-checksum-mismatch-message
  "Copy checksum mismatch")

(def ^:private disruption-write-failure-markers
  ["HA write admission rejected"
   "HA control command timed out"
   "HA write commit confirmation failed"
   "Timed out waiting for single leader"
   "Socket channel is closed."
   "ClosedChannelException"
   "Unable to connect to server:"
   "Connection refused"
   "Connection reset by peer"
   "Broken pipe"
   "Timeout in making request"
   "No space left on device"])

(def ^:private write-disruption-faults
  #{:leader-failover
    :leader-partition
    :quorum-loss
    :leader-pause
    :node-pause
    :multi-node-pause
    :asymmetric-partition
    :degraded-network
    :leader-io-stall
    :leader-disk-full
    :clock-skew-pause
    :clock-skew-leader-fast
    :clock-skew-leader-slow
    :clock-skew-mixed})

(defn- now-ms
  []
  (System/currentTimeMillis))

(defn- safe-read-edn-file
  [path]
  (when (and (string? path) (u/file-exists path))
    (try
      (-> path slurp edn/read-string)
      (catch Throwable _
        nil))))

(defn normalize-storage-fault
  [{:keys [mode stages] :as fault}]
  (let [mode* (keyword (or mode :stall))]
    (when-not (contains? storage-fault-modes mode*)
      (u/raise "Unsupported Jepsen storage fault mode"
               {:mode mode*
                :allowed storage-fault-modes}))
    (assoc fault
           :mode mode*
           :stages (set (or stages storage-fault-default-stages)))))

(defn- remote-runtime-link-fault
  [runtime endpoint]
  (let [{:keys [logical-node endpoint->node]} runtime
        dest-logical-node (get endpoint->node endpoint)
        state             (safe-read-edn-file (:network-state-file runtime))
        blocked-endpoints (set (:blocked-endpoints state))
        profile           (get (:endpoint-profiles state) endpoint)]
    (when (and logical-node dest-logical-node)
      {:src-logical-node logical-node
       :dest-logical-node dest-logical-node
       :blocked? (contains? blocked-endpoints endpoint)
       :profile profile})))

(defn- remote-runtime-storage-fault
  [runtime]
  (some-> (:storage-fault-state-file runtime)
          safe-read-edn-file
          normalize-storage-fault))

(defn- active-remote-storage-fault-target
  [{:keys [remote-runtime-node]} {:keys [db-identity ha-db-identity ha-node-id]}]
  (let [db-identity (or ha-db-identity db-identity)]
    (when-let [runtime (and (string? db-identity)
                            (some? ha-node-id)
                            (remote-runtime-node db-identity ha-node-id))]
      (when-let [fault (remote-runtime-storage-fault runtime)]
        {:cluster-id :remote-runtime
         :logical-node (:logical-node runtime)
         :runtime runtime
         :fault fault}))))

(defn- remote-runtime-snapshot-failpoint
  [{:keys [remote-runtime-node]} {:keys [ha-db-identity ha-node-id]}]
  (some-> (and (string? ha-db-identity)
               (some? ha-node-id)
               (remote-runtime-node ha-db-identity ha-node-id))
          :snapshot-failpoint-file
          safe-read-edn-file
          :mode))

(defn- logical-node-for-ha-state
  [{:keys [cluster-entry-for-db-identity remote-runtime-node]}
   m]
  (if-let [[cluster-id cluster]
           (cluster-entry-for-db-identity (:ha-db-identity m))]
    {:cluster-id cluster-id
     :logical-node (get-in cluster [:node-by-id (:ha-node-id m)])}
    (when-let [runtime (remote-runtime-node (:ha-db-identity m)
                                            (:ha-node-id m))]
      {:cluster-id :remote-runtime
       :logical-node (:logical-node runtime)})))

(defn storage-fault
  [{:keys [clusters]} cluster-id logical-node]
  (get-in @clusters [cluster-id :storage-faults logical-node]))

(defn- active-storage-fault-target
  [{:keys [cluster-entry-for-db-identity] :as deps} {:keys [db-identity ha-node-id]}]
  (when (and (string? db-identity)
             (some? ha-node-id))
    (when-let [[cluster-id cluster] (cluster-entry-for-db-identity db-identity)]
      (when-let [logical-node (get-in cluster [:node-by-id ha-node-id])]
        (when-let [fault (storage-fault deps cluster-id logical-node)]
          {:cluster-id cluster-id
           :logical-node logical-node
           :fault fault})))))

(defn- disk-full-exception
  [cluster-id logical-node stage]
  (ex-info "No space left on device"
           {:type :jepsen/disk-full
            :cluster-id cluster-id
            :logical-node logical-node
            :stage stage}))

(defn maybe-apply-storage-fault!
  [deps {:keys [stage] :as context}]
  (when-let [{:keys [cluster-id logical-node fault runtime]}
             (or (active-storage-fault-target deps context)
                 (active-remote-storage-fault-target deps context))]
    (when (contains? (:stages fault) stage)
      (case (:mode fault)
        :disk-full
        (throw (disk-full-exception cluster-id logical-node stage))

        :stall
        (loop []
          (let [current (if (= :remote-runtime cluster-id)
                          (remote-runtime-storage-fault runtime)
                          (storage-fault deps cluster-id logical-node))]
            (when (and (= :stall (:mode current))
                       (contains? (:stages current) stage))
              (Thread/sleep (long storage-stall-poll-ms))
              (recur))))

        nil))))

(defn blocked-link?
  [{:keys [clusters]} cluster-id src-logical-node dest-logical-node]
  (contains? (get-in @clusters [cluster-id :dropped-links])
             [src-logical-node dest-logical-node]))

(defn- normalized-link-profile
  [profile]
  (let [delay-ms (long (max 0 (or (:delay-ms profile) 0)))
        jitter-ms (long (max 0 (or (:jitter-ms profile) 0)))
        drop-probability (double (max 0.0
                                      (min 1.0
                                           (double
                                            (or (:drop-probability profile)
                                                0.0)))))]
    {:delay-ms delay-ms
     :jitter-ms jitter-ms
     :drop-probability drop-probability}))

(defn- active-link-profile
  [{:keys [clusters]} cluster-id src-logical-node dest-logical-node]
  (get-in @clusters [cluster-id :link-behaviors [src-logical-node dest-logical-node]]))

(defn- blocked-link-exception
  [src-logical-node dest-logical-node endpoint]
  (ConnectException.
   (str "Unable to connect to server: Jepsen partition blocks "
        src-logical-node
        " -> "
        dest-logical-node
        " via "
        endpoint)))

(defn- endpoint-link-fault
  [deps m endpoint]
  (if-let [{:keys [cluster-id logical-node]} (logical-node-for-ha-state deps m)]
    (if (= :remote-runtime cluster-id)
      (remote-runtime-link-fault ((:remote-runtime-node deps)
                                  (:ha-db-identity m)
                                  (:ha-node-id m))
                                 endpoint)
      (let [dest-logical-node (get-in @(-> deps :clusters)
                                      [cluster-id :endpoint->node endpoint])]
        (when (and logical-node dest-logical-node)
          {:cluster-id cluster-id
           :src-logical-node logical-node
           :dest-logical-node dest-logical-node
           :blocked? (blocked-link? deps cluster-id logical-node dest-logical-node)
           :profile (active-link-profile deps
                                         cluster-id
                                         logical-node
                                         dest-logical-node)})))
    nil))

(defn- degraded-link-exception
  [src-logical-node dest-logical-node endpoint]
  (ConnectException.
   (str "Unable to connect to server: Jepsen degraded network dropped "
        src-logical-node
        " -> "
        dest-logical-node
        " via "
        endpoint)))

(defn- maybe-apply-link-fault!
  [{:keys [src-logical-node dest-logical-node blocked? profile]} endpoint]
  (when blocked?
    (throw (blocked-link-exception src-logical-node
                                   dest-logical-node
                                   endpoint)))
  (when profile
    (let [{:keys [delay-ms jitter-ms drop-probability]}
          (normalized-link-profile profile)
          extra-delay-ms (if (pos? jitter-ms)
                           (rand-int (inc (int jitter-ms)))
                           0)
          total-delay-ms (+ delay-ms extra-delay-ms)]
      (when (pos? total-delay-ms)
        (Thread/sleep (long total-delay-ms)))
      (when (or (>= drop-probability 1.0)
                (and (pos? drop-probability)
                     (< (rand) drop-probability)))
        (throw (degraded-link-exception src-logical-node
                                        dest-logical-node
                                        endpoint))))))

(defn partition-aware-fetch-ha-leader-txlog-batch
  [deps db-name m leader-endpoint from-lsn upto-lsn]
  (let [fault (endpoint-link-fault deps m leader-endpoint)]
    (maybe-apply-link-fault! fault leader-endpoint)
    ((:base-fetch-ha-leader-txlog-batch deps)
     db-name m leader-endpoint from-lsn upto-lsn)))

(defn partition-aware-report-ha-replica-floor!
  [deps db-name m leader-endpoint applied-lsn]
  (let [fault (endpoint-link-fault deps m leader-endpoint)]
    (maybe-apply-link-fault! fault leader-endpoint)
    ((:base-report-ha-replica-floor! deps)
     db-name m leader-endpoint applied-lsn)))

(defn partition-aware-fetch-ha-endpoint-snapshot-copy!
  [deps db-name m endpoint dest-dir]
  (let [fault     (endpoint-link-fault deps m endpoint)
        fail-mode (remote-runtime-snapshot-failpoint deps m)
        fetch!    (:base-fetch-ha-endpoint-snapshot-copy! deps)]
    (maybe-apply-link-fault! fault endpoint)
    (case fail-mode
      :snapshot-unavailable
      (throw (ex-info "forced snapshot source failure"
                      {:error snapshot-unavailable-error-code
                       :endpoint endpoint}))

      :db-identity-mismatch
      (let [{:keys [copy-meta] :as result}
            (fetch! db-name m endpoint dest-dir)]
        (assoc result
               :copy-meta
               (assoc (or copy-meta {})
                      :db-name db-name
                      :db-identity "db-mismatch")))

      :manifest-corruption
      (let [{:keys [copy-meta] :as result}
            (fetch! db-name m endpoint dest-dir)]
        (assoc result
               :copy-meta
               (-> (or copy-meta {})
                   (assoc :db-name db-name
                          :db-identity (:ha-db-identity m))
                   (dissoc :snapshot-last-applied-lsn
                           :payload-last-applied-lsn))))

      :checksum-mismatch
      (do
        (fetch! db-name m endpoint dest-dir)
        (throw (ex-info snapshot-checksum-mismatch-message
                        {:expected-checksum "forced-invalid-checksum"
                         :actual-checksum "forced-copy-checksum"})))

      :copy-corruption
      (let [result (fetch! db-name m endpoint dest-dir)]
        (spit (str dest-dir u/+separator+ "data.mdb") "not-an-lmdb-file")
        result)

      (fetch! db-name m endpoint dest-dir))))

(defn network-link-behaviors
  [{:keys [clusters]} cluster-id]
  (get-in @clusters [cluster-id :link-behaviors]))

(defn network-behavior
  [{:keys [clusters]} cluster-id]
  (get-in @clusters [cluster-id :network-behavior]))

(defn- normalized-grudge
  [grudge]
  (into (sorted-map)
        (map (fn [[dest srcs]]
               [dest (vec (sort (set srcs)))]))
        grudge))

(defn- grudge->dropped-links
  [grudge]
  (into #{}
        (mapcat (fn [[dest srcs]]
                  (map (fn [src]
                         [src dest])
                       srcs)))
        grudge))

(defn- nodes->directed-links
  [nodes]
  (for [src nodes
        dest nodes
        :when (not= src dest)]
    [src dest]))

(defn- nodes->link-behaviors
  [nodes profile]
  (let [profile' (normalized-link-profile profile)]
    (into (sorted-map)
          (map (fn [link]
                 [link profile']))
          (nodes->directed-links nodes))))

(defn- link-profile-summary
  [link-behaviors]
  (let [profiles (vec (vals link-behaviors))
        delays   (mapv :delay-ms profiles)
        jitters  (mapv :jitter-ms profiles)
        drops    (mapv :drop-probability profiles)]
    {:distinct-profile-count (count (set profiles))
     :delay-ms {:min (apply min delays)
                :max (apply max delays)}
     :jitter-ms {:min (apply min jitters)
                 :max (apply max jitters)}
     :drop-probability {:min (apply min drops)
                        :max (apply max drops)}}))

(defn- behavior->link-behaviors
  [nodes behavior]
  (cond
    (:link-profiles behavior)
    (into (sorted-map)
          (keep (fn [[[src dest] profile]]
                  (when (and (some? src)
                             (some? dest)
                             (not= src dest)
                             (some #{src} nodes)
                             (some #{dest} nodes))
                    [[src dest] (normalized-link-profile profile)])))
          (:link-profiles behavior))

    (:profile behavior)
    (nodes->link-behaviors nodes (:profile behavior))

    :else
    (nodes->link-behaviors nodes behavior)))

(defn- behavior->network-state
  [nodes behavior link-behaviors]
  (cond
    (:link-profiles behavior)
    (merge (select-keys behavior [:kind])
           {:nodes nodes
            :link-profiles link-behaviors
            :profile-summary (or (:profile-summary behavior)
                                 (link-profile-summary link-behaviors))})

    (:profile behavior)
    {:nodes nodes
     :profile (normalized-link-profile (:profile behavior))}

    :else
    {:nodes nodes
     :profile (normalized-link-profile behavior)}))

(declare heal-network!)

(defn apply-network-shape!
  [{:keys [clusters sync-remote-network-state!]} cluster-id nodes behavior]
  (let [nodes' (->> nodes
                    (filter some?)
                    distinct
                    sort
                    vec)
        link-behaviors (behavior->link-behaviors nodes' behavior)
        network-state  (behavior->network-state nodes'
                                                behavior
                                                link-behaviors)]
    (heal-network! {:clusters clusters
                    :sync-remote-network-state! sync-remote-network-state!}
                   cluster-id)
    (doseq [[[src-logical-node dest-logical-node]
             {:keys [delay-ms jitter-ms drop-probability]}]
            link-behaviors]
      (PartitionFaults/setLinkBehavior cluster-id
                                       src-logical-node
                                       dest-logical-node
                                       delay-ms
                                       jitter-ms
                                       drop-probability))
    (swap! clusters
           (fn [clusters*]
             (if (contains? clusters* cluster-id)
               (-> clusters*
                   (assoc-in [cluster-id :link-behaviors] link-behaviors)
                   (assoc-in [cluster-id :network-behavior] network-state))
               clusters*)))
    (sync-remote-network-state! cluster-id)
    {:cluster-id cluster-id
     :nodes nodes'
     :link-behaviors link-behaviors
     :behavior network-state}))

(defn heal-network!
  [{:keys [clusters sync-remote-network-state!]} cluster-id]
  (PartitionFaults/healCluster cluster-id)
  (swap! clusters
         (fn [clusters*]
           (if (contains? clusters* cluster-id)
             (-> clusters*
                 (assoc-in [cluster-id :network-grudge] (sorted-map))
                 (assoc-in [cluster-id :dropped-links] #{})
                 (assoc-in [cluster-id :link-behaviors] (sorted-map))
                 (assoc-in [cluster-id :network-behavior] nil))
             clusters*)))
  (sync-remote-network-state! cluster-id)
  true)

(defn apply-network-grudge!
  [{:keys [clusters sync-remote-network-state!] :as deps} cluster-id grudge]
  (let [grudge' (normalized-grudge grudge)
        dropped-links (grudge->dropped-links grudge')]
    (heal-network! deps cluster-id)
    (doseq [[src-logical-node dest-logical-node] dropped-links]
      (PartitionFaults/dropLink cluster-id
                                src-logical-node
                                dest-logical-node))
    (swap! clusters
           (fn [clusters*]
             (if (contains? clusters* cluster-id)
               (-> clusters*
                   (assoc-in [cluster-id :network-grudge] grudge')
                   (assoc-in [cluster-id :dropped-links] dropped-links))
               clusters*)))
    (sync-remote-network-state! cluster-id)
    {:cluster-id cluster-id
     :grudge grudge'
     :dropped-links dropped-links}))

(defn network-grudge
  [{:keys [clusters]} cluster-id]
  (get-in @clusters [cluster-id :network-grudge]))

(defn leader-partition-grudge
  [{:keys [wait-for-authority-leader! cluster-state]} cluster-id & [leader]]
  (let [leader     (or leader (:leader (wait-for-authority-leader! cluster-id)))
        live-nodes (-> (cluster-state cluster-id) :live-nodes sort vec)
        followers  (vec (remove #{leader} live-nodes))]
    (when (seq followers)
      (into {leader followers}
            (map (fn [follower]
                   [follower [leader]]))
            followers))))

(defn- random-node-groups
  [nodes]
  (let [shuffled    (vec (shuffle nodes))
        group-count (if (= 2 (count shuffled))
                      2
                      (+ 2 (rand-int (dec (count shuffled)))))
        cut-points  (->> (range 1 (count shuffled))
                         shuffle
                         (take (dec group-count))
                         sort
                         vec)
        bounds      (vec (concat [0] cut-points [(count shuffled)]))]
    (->> (partition 2 1 bounds)
         (mapv (fn [[start end]]
                 (vec (subvec shuffled start end)))))))

(defn- add-blocked-links
  [grudge srcs dests]
  (reduce (fn [grudge' dest]
            (update grudge' dest (fnil into []) srcs))
          grudge
          dests))

(defn- pair-cut->grudge
  [grudge groups {:keys [left-group right-group mode]}]
  (let [left  (nth groups left-group)
        right (nth groups right-group)]
    (case mode
      :left->right
      (add-blocked-links grudge left right)

      :right->left
      (add-blocked-links grudge right left)

      :bidirectional
      (-> grudge
          (add-blocked-links left right)
          (add-blocked-links right left))

      grudge)))

(defn- random-pair-cuts
  [group-count]
  (vec
   (for [left-group (range group-count)
         right-group (range (inc left-group) group-count)
         :let [mode (rand-nth graph-cut-direction-modes)]
         :when (not= :none mode)]
     {:left-group left-group
      :right-group right-group
      :mode mode})))

(defn- fallback-graph-cut
  [nodes]
  (let [[src & rest-nodes] nodes
        groups            [(vector src) (vec rest-nodes)]
        pair-cuts         [{:left-group 0
                            :right-group 1
                            :mode :left->right}]
        grudge            (normalized-grudge
                           (pair-cut->grudge {} groups (first pair-cuts)))]
    {:groups groups
     :pair-cuts pair-cuts
     :grudge grudge
     :dropped-links (grudge->dropped-links grudge)}))

(defn random-graph-cut
  [{:keys [cluster-state]} cluster-id]
  (let [live-nodes (-> (cluster-state cluster-id) :live-nodes sort vec)]
    (when (> (count live-nodes) 1)
      (let [total-links (count (nodes->directed-links live-nodes))]
        (loop [attempt 0]
          (if (>= attempt 32)
            (fallback-graph-cut live-nodes)
            (let [groups         (random-node-groups live-nodes)
                  pair-cuts      (random-pair-cuts (count groups))
                  grudge         (normalized-grudge
                                  (reduce (fn [grudge' pair-cut]
                                            (pair-cut->grudge grudge'
                                                              groups
                                                              pair-cut))
                                          {}
                                          pair-cuts))
                  dropped-links  (grudge->dropped-links grudge)]
              (if (and (seq dropped-links)
                       (< (count dropped-links) total-links))
                {:groups groups
                 :pair-cuts pair-cuts
                 :grudge grudge
                 :dropped-links dropped-links}
                (recur (inc attempt))))))))))

(defn- random-link-profiles
  [links]
  (loop [attempt 0]
    (let [link-profiles (into (sorted-map)
                              (map (fn [link]
                                     [link (rand-nth degraded-network-profile-templates)]))
                              links)]
      (if (or (<= (count links) 1)
              (> (count (set (vals link-profiles))) 1)
              (>= attempt 16))
        link-profiles
        (recur (inc attempt))))))

(defn random-degraded-network-shape
  [{:keys [cluster-state]} cluster-id]
  (let [nodes (-> (cluster-state cluster-id) :live-nodes sort vec)]
    (when (> (count nodes) 1)
      (let [link-profiles (random-link-profiles (nodes->directed-links nodes))]
        {:kind :heterogeneous
         :nodes nodes
         :link-profiles link-profiles
         :profile-summary (link-profile-summary link-profiles)}))))

(defrecord LocalClusterNet [cluster-id deps]
  net.proto/Net
  (drop! [_ _test src dest]
    (apply-network-grudge! deps cluster-id {dest [src]}))
  (heal! [_ _test]
    (heal-network! deps cluster-id))
  (slow! [_ _test]
    (apply-network-shape! deps
                          cluster-id
                          (-> ((:cluster-state deps) cluster-id) :live-nodes sort vec)
                          default-slow-link-profile))
  (slow! [_ _test opts]
    (let [nodes (or (:nodes opts)
                    (-> ((:cluster-state deps) cluster-id) :live-nodes sort vec))
          profile (merge default-slow-link-profile
                         (select-keys opts
                                      [:delay-ms :jitter-ms :drop-probability]))]
      (apply-network-shape! deps cluster-id nodes profile)))
  (flaky! [_ _test]
    (apply-network-shape! deps
                          cluster-id
                          (-> ((:cluster-state deps) cluster-id) :live-nodes sort vec)
                          default-flaky-link-profile))
  (fast! [_ _test]
    (heal-network! deps cluster-id))
  (shape! [_ _test nodes behavior]
    (apply-network-shape! deps cluster-id nodes behavior))

  net.proto/PartitionAll
  (drop-all! [_ _test grudge]
    (apply-network-grudge! deps cluster-id grudge)))

(defn transport-failure?
  [e]
  (boolean
    (some
      (fn [cause]
        (let [message (ex-message cause)
              data    (ex-data cause)]
          (or (instance? ClosedChannelException cause)
              (instance? ConnectException cause)
              (= :open-conn (:phase data))
              (and (string? message)
                   (or (str/includes? message "Socket channel is closed.")
                       (str/includes? message "ClosedChannelException")
                       (str/includes? message "Unable to connect to server:")
                       (str/includes? message "Connection refused")
                       (str/includes? message "Connection reset by peer")
                       (str/includes? message "Broken pipe")
                       (str/includes? message "Timeout in making request"))))))
      (take-while some? (iterate ex-cause e)))))

(defn write-disruption-fault-active?
  [test]
  (boolean (some write-disruption-faults (:datalevin/nemesis-faults test))))

(defn- disruption-write-failure-message
  [error]
  (cond
    (string? error)
    error

    (keyword? error)
    (name error)

    (vector? error)
    (some->> error
             (keep disruption-write-failure-message)
             first)

    (map? error)
    (or (disruption-write-failure-message (:message error))
        (disruption-write-failure-message (:error error)))

    (some? error)
    (str error)

    :else
    nil))

(defn expected-disruption-write-failure?
  [test error]
  (and (write-disruption-fault-active? test)
       (when-let [message (disruption-write-failure-message error)]
         (some #(str/includes? message %)
               disruption-write-failure-markers))))

(defn wedge-node-storage!
  [{:keys [clusters write-remote-content!]} cluster-id logical-node fault]
  (locking clusters
    (when-let [cluster (get @clusters cluster-id)]
      (when-not (contains? (:live-nodes cluster) logical-node)
        (u/raise "Cannot wedge storage on unavailable Jepsen node"
                 {:cluster-id cluster-id
                  :logical-node logical-node}))
      (let [fault* (assoc (normalize-storage-fault fault)
                          :faulted-at-ms (now-ms))]
        (when (:remote? cluster)
          (let [{:keys [ssh node-by-name]} cluster
                node (get node-by-name logical-node)]
            (write-remote-content! ssh
                                   node
                                   (remote/storage-fault-state-file node)
                                   (pr-str fault*))))
        (swap! clusters assoc-in [cluster-id :storage-faults logical-node] fault*)
        fault*))))

(defn heal-node-storage!
  [{:keys [clusters delete-remote-path!] :as deps} cluster-id logical-node]
  (locking clusters
    (let [fault (storage-fault deps cluster-id logical-node)]
      (when-let [{:keys [remote? ssh node-by-name]} (get @clusters cluster-id)]
        (when remote?
          (when-let [node (get node-by-name logical-node)]
            (delete-remote-path! ssh
                                 node
                                 (remote/storage-fault-state-file node)))))
      (swap! clusters update-in [cluster-id :storage-faults] dissoc logical-node)
      fault)))

(defn set-node-snapshot-failpoint!
  [{:keys [clusters write-remote-content!]} cluster-id logical-node mode]
  (when-let [{:keys [remote? ssh node-by-name]} (get @clusters cluster-id)]
    (when remote?
      (when-let [node (get node-by-name logical-node)]
        (write-remote-content! ssh
                               node
                               (remote/snapshot-failpoint-file node)
                               (pr-str {:mode mode}))
        true))))

(defn clear-node-snapshot-failpoint!
  [{:keys [clusters delete-remote-path!]} cluster-id logical-node]
  (when-let [{:keys [remote? ssh node-by-name]} (get @clusters cluster-id)]
    (when remote?
      (when-let [node (get node-by-name logical-node)]
        (delete-remote-path! ssh
                             node
                             (remote/snapshot-failpoint-file node))
        true))))

(defn set-fencing-hook-mode!
  [{:keys [clusters write-remote-content!]} cluster-id mode]
  (when-let [{:keys [remote? ssh nodes node-by-name]} (get @clusters cluster-id)]
    (when remote?
      (doseq [logical-node (map :logical-node nodes)
              :let [node (get node-by-name logical-node)]
              :when node]
        (write-remote-content! ssh
                               node
                               (remote/fencing-mode-file node)
                               (str (name mode) "\n")))
      true)))
