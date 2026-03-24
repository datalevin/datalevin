(ns datalevin.jepsen.core
  (:require
   [clojure.string :as str]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.nemesis :as datalevin.nemesis]
   [datalevin.jepsen.remote :as remote]
   [datalevin.jepsen.workload.append :as append]
   [datalevin.jepsen.workload.append-cas :as append-cas]
   [datalevin.jepsen.workload.bank :as bank]
   [datalevin.jepsen.workload.degraded-rejoin :as degraded-rejoin]
   [datalevin.jepsen.workload.fencing :as fencing]
   [datalevin.jepsen.workload.fencing-retry :as fencing-retry]
   [datalevin.jepsen.workload.giant-values :as giant-values]
   [datalevin.jepsen.workload.grant :as grant]
   [datalevin.jepsen.workload.identity-upsert :as identity-upsert]
   [datalevin.jepsen.workload.index-consistency :as index-consistency]
   [datalevin.jepsen.workload.internal :as internal]
   [datalevin.jepsen.workload.membership-drift :as membership-drift]
   [datalevin.jepsen.workload.rejoin-bootstrap :as rejoin-bootstrap]
   [datalevin.jepsen.workload.register :as register]
   [datalevin.jepsen.workload.udf-readiness :as udf-readiness]
   [datalevin.jepsen.workload.witness-topology :as witness-topology]
   [datalevin.jepsen.workload.tx-fn-register :as tx-fn-register]
   [jepsen.checker :as checker]
   [jepsen.checker.timeline :as timeline]
   [jepsen.generator :as gen]
   [jepsen.tests :as tests])
  (:import
   [java.util UUID]))

(def workloads
  {:append append/workload
   :append-cas append-cas/workload
   :bank bank/workload
   :degraded-rejoin degraded-rejoin/workload
   :fencing fencing/workload
   :fencing-retry fencing-retry/workload
   :giant-values giant-values/workload
   :grant grant/workload
   :identity-upsert identity-upsert/workload
   :index-consistency index-consistency/workload
   :internal internal/workload
   :membership-drift membership-drift/workload
   :membership-drift-live membership-drift/live-workload
   :rejoin-bootstrap rejoin-bootstrap/workload
   :snapshot-checksum-rejoin degraded-rejoin/checksum-workload
   :snapshot-copy-corruption-rejoin degraded-rejoin/copy-corruption-workload
   :snapshot-db-identity-rejoin degraded-rejoin/db-identity-workload
   :snapshot-manifest-corruption-rejoin degraded-rejoin/manifest-corruption-workload
   :register register/workload
   :udf-readiness udf-readiness/workload
   :witness-topology witness-topology/workload
   :tx-fn-register tx-fn-register/workload})

(defn parse-nemesis-spec
  [spec]
  (->> (str/split spec #",")
       (remove str/blank?)
       (map keyword)
       (mapcat datalevin.nemesis/expand-fault)
       vec))

(def supported-nemeses
  datalevin.nemesis/supported-faults)

(defn- validate-nemesis-compatibility!
  [{:keys [control-backend nemesis]}]
  (when (and (some #{:leader-failover
                     :leader-pause
                     :node-pause
                     :multi-node-pause
                     :leader-partition
                     :asymmetric-partition
                     :degraded-network
                     :leader-io-stall
                     :leader-disk-full
                     :quorum-loss
                     :clock-skew-pause
                     :clock-skew-leader-fast
                     :clock-skew-leader-slow
                     :clock-skew-mixed
                     :follower-rejoin} nemesis)
             (not= :sofa-jraft control-backend))
    (throw (ex-info
            "HA disruption nemeses currently require --control-backend sofa-jraft"
            {:nemesis nemesis
             :control-backend control-backend}))))

(defn- compose-generator-phases
  [timed-gen workload-final-generator nemesis-final-generator]
  (cond-> [timed-gen]
    nemesis-final-generator
    (conj (gen/nemesis nemesis-final-generator))

    workload-final-generator
    (conj (gen/clients workload-final-generator))))

(def ^:private remote-unsupported-workloads
  #{:degraded-rejoin
    :membership-drift
    :membership-drift-live
    :rejoin-bootstrap
    :snapshot-checksum-rejoin
    :snapshot-copy-corruption-rejoin
    :snapshot-db-identity-rejoin
    :snapshot-manifest-corruption-rejoin
    :fencing-retry})

(def ^:private remote-unsupported-nemeses
  #{:leader-io-stall
    :leader-disk-full
    :clock-skew-pause
    :clock-skew-leader-fast
    :clock-skew-leader-slow
    :clock-skew-mixed})

(defn- validate-remote-runner!
  [config workload-name nemesis-faults _topology]
  (when (contains? remote-unsupported-workloads workload-name)
    (throw (ex-info
            "This Jepsen workload is not yet supported by the remote runner"
            {:workload workload-name
             :remote-config (:db-name config)})))
  (when-let [fault (first (filter remote-unsupported-nemeses nemesis-faults))]
    (throw (ex-info
            "This Jepsen nemesis is not yet supported by the remote runner"
            {:fault fault
             :nemesis nemesis-faults
             :workload workload-name})))
  (when-not (string? (:repo-root config))
    (throw (ex-info
            "Remote Jepsen runner requires :repo-root in the remote config"
            {:workload workload-name
             :db-name (:db-name config)}))))

(defn- remote-datalevin-test
  [opts]
  (let [cluster-id     (str (UUID/randomUUID))
        config         (-> (:remote-config opts)
                           remote/read-config
                           (remote/validate-config! workloads))
        workload-name  (:workload config)
        workload       (remote/config-workload config workloads)
        topology       (remote/workload-topology config workload)
        nodes          (vec (or (seq (:nodes workload))
                                (map :logical-node (:data-nodes topology))))
        control-nodes  (vec (or (seq (:datalevin/control-nodes workload))
                                (map :logical-node (:control-nodes topology))))
        rate           (double (:rate opts))
        time-limit     (:time-limit opts)
        nemesis-faults (:nemesis opts)
        _              (validate-nemesis-compatibility!
                        (assoc opts :control-backend (:control-backend config)))
        _              (validate-remote-runner! config
                                               workload-name
                                               nemesis-faults
                                               topology)
        {:keys [nemesis generator final-generator]}
        (datalevin.nemesis/nemesis-package {:faults nemesis-faults})
        workload-final-generator
        (:final-generator workload)
        client-gen     (->> (:generator workload)
                            (gen/stagger (/ rate)))
        combined-gen   (if generator
                         (gen/clients client-gen generator)
                         (gen/clients client-gen))
        timed-gen      (->> combined-gen
                            (gen/time-limit time-limit))
        phases         (compose-generator-phases timed-gen
                                                workload-final-generator
                                                final-generator)
        ssh-opts       (merge {:username "root"
                               :password "root"
                               :strict-host-key-checking false}
                              (:ssh opts))]
    (merge tests/noop-test
           opts
           {:name (str (name workload-name) " remote")
            :nodes nodes
            :db (local/remote-db cluster-id
                                 {:config config
                                  :config-path (:remote-config opts)
                                  :ssh ssh-opts
                                  :topology topology
                                  :workload workload})
            :client (:client workload)
            :nemesis nemesis
            :checker (checker/compose
                      {:timeline   (timeline/html)
                       :exceptions (checker/unhandled-exceptions)
                       :workload   (:checker workload)})
            :generator (apply gen/phases phases)
            :schema (:schema workload)
            :db-name (:db-name config)
            :control-backend (:control-backend config)
            :datalevin/cluster-opts (:datalevin/cluster-opts workload)
            :datalevin/server-runtime-opts-fn
            (:datalevin/server-runtime-opts-fn workload)
            :datalevin/control-nodes control-nodes
            :ssh ssh-opts
            :datalevin/nemesis-faults nemesis-faults
            :datalevin/cluster-id cluster-id
            :datalevin/remote-config (:remote-config opts)})))

(defn datalevin-test
  [opts]
  (if (:remote-config opts)
    (remote-datalevin-test opts)
    (let [_              (validate-nemesis-compatibility! opts)
        cluster-id     (str (UUID/randomUUID))
        workload-name  (:workload opts)
        workload       ((workloads workload-name) opts)
        nodes          (vec (or (seq (:nodes workload))
                                (seq (:nodes opts))
                                local/default-nodes))
        control-nodes  (vec (or (seq (:datalevin/control-nodes workload))
                                (seq (:datalevin/control-nodes opts))
                                nodes))
        rate           (double (:rate opts))
        time-limit     (:time-limit opts)
        nemesis-faults (:nemesis opts)
        {:keys [nemesis generator final-generator]}
        (datalevin.nemesis/nemesis-package {:faults nemesis-faults})
        workload-final-generator
        (:final-generator workload)
        client-gen     (->> (:generator workload)
                            (gen/stagger (/ rate)))
        combined-gen   (if generator
                         (gen/clients client-gen generator)
                         (gen/clients client-gen))
        timed-gen      (->> combined-gen
                            (gen/time-limit time-limit))
        phases         (compose-generator-phases timed-gen
                                                workload-final-generator
                                                final-generator)
        ssh-opts       (assoc (merge {:username "root"
                                      :password "root"
                                      :strict-host-key-checking false}
                                     (:ssh opts))
                         :dummy? true)]
    (merge tests/noop-test
           opts
           {:name (str (name workload-name) " "
                       (if (= :sofa-jraft (:control-backend opts))
                         "sofa-jraft"
                         "in-memory"))
            :nodes nodes
            :db (local/db cluster-id)
            :net (local/net cluster-id)
            :client (:client workload)
            :nemesis nemesis
            :checker (checker/compose
                      {:timeline   (timeline/html)
                       :exceptions (checker/unhandled-exceptions)
                       :workload   (:checker workload)})
            :generator (apply gen/phases phases)
            :schema (:schema workload)
            :db-name (:db-name opts)
            :control-backend (:control-backend opts)
            :datalevin/cluster-opts (:datalevin/cluster-opts workload)
            :datalevin/server-runtime-opts-fn
            (:datalevin/server-runtime-opts-fn workload)
            :datalevin/control-nodes control-nodes
            :ssh ssh-opts
            :datalevin/nemesis-faults nemesis-faults
            :datalevin/cluster-id cluster-id}))))
