(ns datalevin.jepsen.core
  (:require
   [clojure.string :as str]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.nemesis :as datalevin.nemesis]
   [datalevin.jepsen.workload.append :as append]
   [datalevin.jepsen.workload.append-cas :as append-cas]
   [datalevin.jepsen.workload.bank :as bank]
   [datalevin.jepsen.workload.fencing :as fencing]
   [datalevin.jepsen.workload.giant-values :as giant-values]
   [datalevin.jepsen.workload.grant :as grant]
   [datalevin.jepsen.workload.identity-upsert :as identity-upsert]
   [datalevin.jepsen.workload.index-consistency :as index-consistency]
   [datalevin.jepsen.workload.internal :as internal]
   [datalevin.jepsen.workload.rejoin-bootstrap :as rejoin-bootstrap]
   [datalevin.jepsen.workload.register :as register]
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
   :fencing fencing/workload
   :giant-values giant-values/workload
   :grant grant/workload
   :identity-upsert identity-upsert/workload
   :index-consistency index-consistency/workload
   :internal internal/workload
   :rejoin-bootstrap rejoin-bootstrap/workload
   :register register/workload
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

(defn datalevin-test
  [opts]
  (let [_              (validate-nemesis-compatibility! opts)
        cluster-id     (str (UUID/randomUUID))
        nodes          (vec (or (seq (:nodes opts)) local/default-nodes))
        workload-name  (:workload opts)
        workload       ((workloads workload-name) opts)
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
            :ssh ssh-opts
            :datalevin/nemesis-faults nemesis-faults
            :datalevin/cluster-id cluster-id})))
