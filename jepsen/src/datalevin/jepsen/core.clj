(ns datalevin.jepsen.core
  (:refer-clojure :exclude [run!])
  (:require
   [clojure.java.shell :refer [sh]]
   [clojure.string :as str]
   [clojure.tools.logging :refer [info warn]]
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
   [jepsen.control :as control]
   [jepsen.core :as jepsen]
   [jepsen.db :as jdb]
   [jepsen.generator :as gen]
   [jepsen.os :as os]
   [jepsen.store :as store]
   [jepsen.tests :as tests]
   [jepsen.util :as jutil]
   [slingshot.slingshot :refer [try+]])
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
  #{})

(def ^:private remote-unsupported-nemeses
  #{})

(def ^:private default-ssh-opts
  {:dummy? false
   :username "root"
   :password "root"
   :private-key-path nil
   :strict-host-key-checking false})

(defn- explicit-ssh-overrides
  [ssh-opts]
  (into {}
        (keep (fn [[k v]]
                (when (not= v (get default-ssh-opts k ::missing))
                  [k v])))
        ssh-opts))

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
        session-nodes  (->> (:data-nodes topology)
                            (remove #(true? (:controller-local? %)))
                            (mapv :logical-node))
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
        ssh-opts       (merge default-ssh-opts
                              (:ssh config)
                              (explicit-ssh-overrides (:ssh opts)))]
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
            :datalevin/session-nodes session-nodes
            :remote (or (:remote opts) control/*remote* control/ssh)
            :ssh ssh-opts
            :datalevin/nemesis-faults nemesis-faults
            :datalevin/cluster-id cluster-id
            :datalevin/remote-config (:remote-config opts)})))

(defn- session-nodes
  [test]
  (vec (or (seq (:datalevin/session-nodes test))
           (seq (:nodes test))
           [])))

(defn- remote-runner-test?
  [test]
  (boolean (:datalevin/remote-config test)))

(defn- invoke-db-on-nodes!
  [test nodes f]
  (dorun
   (jutil/real-pmap
    (fn [node]
      (f node))
    nodes)))

(defn- control-on-session-nodes
  ([test f]
   (control-on-session-nodes test (session-nodes test) f))
  ([test nodes f]
   (if (seq nodes)
     (control/on-nodes test nodes f)
     {})))

(defn- cycle-db!
  [test]
  (let [db    (:db test)
        nodes (vec (:nodes test))]
    (loop [tries jdb/cycle-tries]
      (info "Tearing down DB")
      (if (remote-runner-test? test)
        (invoke-db-on-nodes! test nodes #(jdb/teardown! db test %))
        (control-on-session-nodes test (partial jdb/teardown! db)))

      (if (= :retry
             (try+
               (info "Setting up DB")
               (if (remote-runner-test? test)
                 (invoke-db-on-nodes! test nodes #(jdb/setup! db test %))
                 (control-on-session-nodes test (partial jdb/setup! db)))

               (when (satisfies? jdb/Primary db)
                 (let [primary-node (first nodes)]
                   (when primary-node
                     (info "Setting up primary" primary-node)
                     (if (remote-runner-test? test)
                       (jdb/setup-primary! db test primary-node)
                       (control-on-session-nodes
                        test
                        [primary-node]
                        (partial jdb/setup-primary! db))))))

               nil
               (catch [:type :jepsen.db/setup-failed] e
                 (if (< 1 tries)
                   (do
                     (warn (:throwable &throw-context)
                           "Unable to set up database; retrying...")
                     :retry)
                   (throw e)))))
        (recur (dec tries))))))

(defn- log-test-start!
  [test]
  (let [git-head (sh "git" "rev-parse" "HEAD")]
    (when (zero? (:exit git-head))
      (let [head   (str/trim-newline (:out git-head))
            clean? (-> (sh "git" "status" "--porcelain=v1")
                       :out
                       str/blank?)]
        (info (str "Test version " head
                   (when-not clean? " (plus uncommitted changes)"))))))
  (when-let [argv (:argv test)]
    (info (str "Command line:\n"
               (->> argv
                    (map control/escape)
                    (list* "lein" "run")
                    (str/join " ")))))
  (info "Running test:"
        (pr-str (cond-> {:name (:name test)
                         :db-name (:db-name test)
                         :nodes (vec (:nodes test))
                         :session-nodes (session-nodes test)
                         :control-nodes (vec (:datalevin/control-nodes test))
                         :nemesis (vec (:datalevin/nemesis-faults test))}
                  (:datalevin/remote-config test)
                  (assoc :remote-config (:datalevin/remote-config test))))))

(defmacro with-sessions
  [[test-sym test] & body]
  `(let [test# ~test
         session-nodes# (session-nodes test#)]
     (control/with-remote (:remote test#)
       (control/with-ssh (:ssh test#)
         (jepsen/with-resources [sessions#
                                 (bound-fn* control/session)
                                 control/disconnect
                                 session-nodes#]
           (let [~test-sym (assoc test#
                                  :sessions (->> sessions#
                                                 (map vector session-nodes#)
                                                 (into {})))]
             ~@body))))))

(defmacro with-os
  [test & body]
  `(try
     (control-on-session-nodes ~test (partial os/setup! (:os ~test)))
     ~@body
     (finally
       (control-on-session-nodes ~test (partial os/teardown! (:os ~test))))))

(defmacro with-db
  [test & body]
  `(try
     (jepsen/with-log-snarfing ~test
       (cycle-db! ~test)
       ~@body)
     (finally
       (when-not (:leave-db-running? ~test)
         (if (remote-runner-test? ~test)
           (invoke-db-on-nodes! ~test
                                (vec (:nodes ~test))
                                #(jdb/teardown! (:db ~test) ~test %))
           (control-on-session-nodes ~test
                                     (partial jdb/teardown! (:db ~test))))))))

(defmacro with-logging
  [test & body]
  `(try
     (store/start-logging! ~test)
     (log-test-start! ~test)
     ~@body
     (catch Throwable t#
       (warn t# "Test crashed!")
       (throw t#))
     (finally
       (store/stop-logging!))))

(defn run!
  [test]
  (jutil/with-thread-name "jepsen test runner"
    (let [test (jepsen/prepare-test test)]
      (with-logging test
        (store/with-handle [test test]
          (let [test (if (:name test)
                       (store/save-0! test)
                       test)
                test (with-sessions [test test]
                       (let [test (with-os test
                                    (with-db test
                                      (jutil/with-relative-time
                                        (let [test (-> (jepsen/run-case! test)
                                                       (dissoc :barrier
                                                               :sessions))
                                              _    (info "Run complete, writing")
                                              test (if (:name test)
                                                     (store/save-1! test)
                                                     test)]
                                          test))))]
                         (jepsen/analyze! test)))]
            (jepsen/log-results test)))))))

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
        ssh-opts       (assoc (merge default-ssh-opts
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
            :remote (or (:remote opts) control/*remote* control/ssh)
            :ssh ssh-opts
            :datalevin/nemesis-faults nemesis-faults
            :datalevin/cluster-id cluster-id}))))
