;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.control
  "Consensus control-plane lease authority protocol and in-memory adapter."
  (:require
   [clojure.string :as s]
   [datalevin.ha.lease :as lease]
   [datalevin.util :as u]
   [taoensso.nippy :as nippy]
   [taoensso.timbre :as log])
  (:import
   [com.alipay.sofa.jraft Closure Iterator Node RaftGroupService Status]
   [com.alipay.sofa.jraft.closure ReadIndexClosure]
   [com.alipay.sofa.jraft.conf Configuration]
   [com.alipay.sofa.jraft.core StateMachineAdapter]
   [com.alipay.sofa.jraft.entity PeerId Task]
   [com.alipay.sofa.jraft.error RaftError]
   [com.alipay.sofa.jraft.option NodeOptions RpcOptions]
   [com.alipay.sofa.jraft.rpc RpcContext RpcProcessor RpcClient
    RpcRequests$ErrorResponse ProtobufMsgFactory]
   [com.alipay.sofa.jraft.storage.snapshot SnapshotReader SnapshotWriter]
   [com.alipay.sofa.jraft.util RpcFactoryHelper]
   [datalevin.ha LMDBJRaftServiceFactory]
   [java.io File]
   [java.net ConnectException InetSocketAddress NoRouteToHostException
    Socket]
   [java.nio ByteBuffer]
   [java.nio.file AtomicMoveNotSupportedException Files Paths
    StandardCopyOption]
   [java.util Base64]))

(defprotocol ILeaseAuthority
  (start-authority! [this] "Start authority lifecycle resources.")
  (stop-authority! [this] "Stop authority lifecycle resources.")
  (read-lease [this db-identity]
    "Linearizable read of authoritative lease and version for db-identity.")
  (try-acquire-lease [this req]
    "CAS lease acquisition attempt using observed lease/version.")
  (renew-lease [this req]
    "Owner + term guarded lease renew.")
  (release-lease [this req]
    "Owner + term guarded lease release.")
  (read-membership-hash [this]
    "Read authoritative membership hash (nil when unset).")
  (init-membership-hash! [this membership-hash]
    "Initialize authoritative membership hash once, compare-only afterwards.")
  (read-voters [this]
    "Read authoritative control-plane voter peer IDs.")
  (replace-voters! [this voters]
    "Replace authoritative control-plane voters using a manual reconfiguration."))

(declare validated-peer-ids!)

(defn current-term
  "Helper read: current authoritative term for db-identity, defaulting to 0."
  [authority db-identity]
  (lease/observed-term (:lease (read-lease authority db-identity))))

(defn owner
  "Helper read: current authoritative owner node-id for db-identity."
  [authority db-identity]
  (:leader-node-id (:lease (read-lease authority db-identity))))

(defn lease-key
  "Canonical authoritative lease key."
  [group-id db-identity]
  (lease/lease-key group-id db-identity))

(defn membership-hash-key
  "Canonical authoritative membership-hash key."
  [group-id]
  (lease/membership-hash-key group-id))

(defn- blank-state
  []
  {:leases {}
   :membership-hash nil
   :voters []})

(defn ^:redef control-now-ms
  []
  (System/currentTimeMillis))

(def ^:dynamic *in-memory-cas-timeout-ms*
  5000)

(def ^:private max-in-memory-cas-backoff-ms
  10)

(defn ^:redef in-memory-cas-now-ms
  []
  (System/currentTimeMillis))

(defn ^:redef commit-state-compare-and-set!
  [state-atom old-state new-state]
  (compare-and-set! state-atom old-state new-state))

(defn- in-memory-cas-backoff-ms ^long
  [attempt]
  (let [attempt (max 0 (int attempt))
        delay-ms (bit-shift-left 1 (min attempt 3))
        max-backoff-ms (long max-in-memory-cas-backoff-ms)]
    (if (> (long delay-ms) max-backoff-ms)
      max-backoff-ms
      (long delay-ms))))

(defn ^:redef sleep-in-memory-cas-retry!
  [attempt]
  (Thread/sleep (long (in-memory-cas-backoff-ms attempt))))

(defonce ^:private in-memory-groups
  (atom {}))

(defonce ^:private in-memory-group-scope-seq
  (atom 0))

(def ^:dynamic *in-memory-group-scope*
  :global)

(defn fresh-in-memory-group-scope
  "Allocate a fresh in-memory authority scope token.

  Authorities created under different scope tokens do not share the same
  backing group-state atoms even when they reuse the same group-id."
  []
  (keyword "datalevin.ha.control.scope"
           (str (swap! in-memory-group-scope-seq inc))))

(defn- group-state
  ([group-id]
   (group-state *in-memory-group-scope* group-id))
  ([scope-id group-id]
   (or (get-in @in-memory-groups [scope-id group-id])
      (let [state (atom (blank-state))]
        (get-in (swap! in-memory-groups
                       (fn [m]
                         (if (contains? (get m scope-id {}) group-id)
                           m
                           (assoc-in m [scope-id group-id] state))))
                [scope-id group-id])))))

(defn reset-in-memory-groups!
  "Clear shared in-memory authority state for one scope.

  This is intended for test fixtures so separate tests can safely reuse
  deterministic in-memory group IDs without inheriting leases or membership
  from earlier tests. When called with no argument it clears the current
  `*in-memory-group-scope*` only."
  ([] (reset-in-memory-groups! *in-memory-group-scope*))
  ([scope-id]
   (swap! in-memory-groups dissoc scope-id)
   nil))

(defn with-fresh-in-memory-group-scope
  "Run `f` with a fresh isolated in-memory authority scope."
  [f]
  (let [scope-id (fresh-in-memory-group-scope)]
    (binding [*in-memory-group-scope* scope-id]
      (try
        (f)
        (finally
          (reset-in-memory-groups! scope-id))))))

(defn current-in-memory-group-scope
  "Return the current in-memory authority scope token."
  []
  *in-memory-group-scope*)

(defn- non-blank-string?
  [x]
  (and (string? x) (not (s/blank? x))))

(defn- positive-int?
  [x]
  (and (integer? x) (pos? ^long x)))

(defn- non-negative-int?
  [x]
  (and (integer? x) (not (neg? ^long x))))

(defn- require-non-blank-string!
  [x where]
  (when-not (non-blank-string? x)
    (u/raise "HA control value must be a non-blank string"
             {:error :ha/control-invalid-request
              :where where
              :value x})))

(defn- require-positive-int!
  [x where]
  (when-not (positive-int? x)
    (u/raise "HA control value must be a positive integer"
             {:error :ha/control-invalid-request
              :where where
              :value x})))

(defn- require-integer!
  [x where]
  (when-not (integer? x)
    (u/raise "HA control value must be an integer"
             {:error :ha/control-invalid-request
              :where where
              :value x})))

(defn- validate-acquire-request!
  [{:keys [db-identity leader-node-id leader-endpoint lease-renew-ms
           lease-timeout-ms now-ms observed-version observed-lease]}]
  (require-non-blank-string! db-identity :db-identity)
  (require-positive-int! leader-node-id :leader-node-id)
  (require-non-blank-string! leader-endpoint :leader-endpoint)
  (require-positive-int! lease-renew-ms :lease-renew-ms)
  (require-positive-int! lease-timeout-ms :lease-timeout-ms)
  (require-integer! now-ms :now-ms)
  (when (some? observed-version)
    (when-not (non-negative-int? observed-version)
      (u/raise "HA observed-version must be a non-negative integer"
               {:error :ha/control-invalid-request
                :where :observed-version
                :value observed-version})))
  (when (and observed-lease
             (some? (:db-identity observed-lease))
             (not= db-identity (:db-identity observed-lease)))
    (u/raise "HA observed lease db-identity mismatch"
             {:error :ha/control-invalid-request
              :where :observed-lease
              :db-identity db-identity
              :observed-db-identity (:db-identity observed-lease)})))

(defn- validate-renew-request!
  [{:keys [db-identity leader-node-id leader-endpoint term lease-renew-ms
           lease-timeout-ms now-ms]}]
  (require-non-blank-string! db-identity :db-identity)
  (require-positive-int! leader-node-id :leader-node-id)
  (require-non-blank-string! leader-endpoint :leader-endpoint)
  (require-positive-int! term :term)
  (require-positive-int! lease-renew-ms :lease-renew-ms)
  (require-positive-int! lease-timeout-ms :lease-timeout-ms)
  (require-integer! now-ms :now-ms))

(defn- validate-release-request!
  [{:keys [db-identity leader-node-id term]}]
  (require-non-blank-string! db-identity :db-identity)
  (require-positive-int! leader-node-id :leader-node-id)
  (require-positive-int! term :term))

(defn- validate-clock-skew-budget!
  [clock-skew-budget-ms where]
  (when (some? clock-skew-budget-ms)
    (require-positive-int! clock-skew-budget-ms where))
  clock-skew-budget-ms)

(defn- authority-clock-skew-result
  [now-ms authority-now-ms clock-skew-budget-ms]
  (when (some? clock-skew-budget-ms)
    (let [authority-now-ms (do
                             (require-integer! authority-now-ms
                                               :authority-now-ms)
                             (long authority-now-ms))
          clock-skew-budget-ms
          (long (validate-clock-skew-budget!
                 clock-skew-budget-ms
                 :clock-skew-budget-ms))
          clock-skew-ms
          (Math/abs (long (- (long now-ms) authority-now-ms)))]
      (when (> clock-skew-ms clock-skew-budget-ms)
        {:ok? false
         :reason :clock-skew-exceeded
         :now-ms (long now-ms)
         :authority-now-ms authority-now-ms
         :clock-skew-ms clock-skew-ms
         :clock-skew-budget-ms clock-skew-budget-ms}))))

(defn- stamp-lease-command
  [cmd clock-skew-budget-ms]
  ;; For JRaft-backed authorities this timestamp must be fixed by the leader
  ;; before the command is appended to the replicated log. Restamping inside
  ;; FSM apply would make different peers derive different lease records based
  ;; on their local clocks. The tradeoff is intentionally conservative: the
  ;; effective lease duration is shortened by Raft commit latency.
  (cond-> cmd
    (contains? #{:try-acquire-lease :renew-lease} (:op cmd))
    (assoc :authority-now-ms (long (control-now-ms))
           :clock-skew-budget-ms
           (some-> clock-skew-budget-ms
                   (validate-clock-skew-budget! :clock-skew-budget-ms)
                   long))))

(defn ^:redef authoritative-command
  [authority cmd]
  (stamp-lease-command cmd (:clock-skew-budget-ms authority)))

(defn- require-authority-now-ms
  [authority-now-ms]
  (require-integer! authority-now-ms :authority-now-ms)
  (long authority-now-ms))

(defn- lease-entry
  [state db-identity]
  (get-in state [:leases db-identity] {:lease nil :version 0}))

(defn- running?
  [running-v]
  (true? @running-v))

(defn- ensure-running!
  [running-v]
  (when-not (running? running-v)
    (u/raise "HA lease authority is not started"
             {:error :ha/control-not-started})))

(defn- commit-state-transition!
  [state-atom transition-fn]
  (let [timeout-ms (long (max 1 (long *in-memory-cas-timeout-ms*)))
        deadline   (+ (long (in-memory-cas-now-ms)) timeout-ms)]
    (loop [attempt 0]
    (let [old-state @state-atom
          {:keys [state result]} (transition-fn old-state)]
      (if (commit-state-compare-and-set! state-atom old-state state)
        result
        (let [attempt (unchecked-inc-int attempt)]
          (when (>= (long (in-memory-cas-now-ms)) deadline)
            (u/raise "HA in-memory control CAS contention timed out"
                     {:error :ha/control-cas-timeout
                      :attempt attempt}))
          (sleep-in-memory-cas-retry! (dec attempt))
          (recur attempt)))))))

(defn ^:redef in-memory-linearizable-state-snapshot!
  [state-atom]
  (commit-state-transition!
   state-atom
   (fn [snapshot]
     {:state snapshot
      :result snapshot})))

(declare apply-state-command!)

(defrecord InMemoryLeaseAuthority [group-id state running-v initial-voters
                                   clock-skew-budget-ms]
  ILeaseAuthority
  (start-authority! [this]
    (when (seq initial-voters)
      (swap! state
             (fn [s]
               (if (seq (:voters s))
                 s
                 (assoc s :voters initial-voters)))))
    (vreset! running-v true)
    this)

  (stop-authority! [this]
    (vreset! running-v false)
    this)

  (read-lease [this db-identity]
    (ensure-running! running-v)
    (lease/validate-lease-key! group-id db-identity)
    (let [{:keys [lease version]}
          (lease-entry (in-memory-linearizable-state-snapshot! state)
                       db-identity)]
      {:lease lease
       :version version}))

  (try-acquire-lease [_ {:keys [db-identity leader-node-id leader-endpoint
                                lease-renew-ms lease-timeout-ms
                                leader-last-applied-lsn now-ms
                                observed-version observed-lease]}]
    (ensure-running! running-v)
    (validate-acquire-request!
      {:db-identity db-identity
       :leader-node-id leader-node-id
       :leader-endpoint leader-endpoint
       :lease-renew-ms lease-renew-ms
       :lease-timeout-ms lease-timeout-ms
       :now-ms now-ms
       :observed-version observed-version
       :observed-lease observed-lease})
    (apply-state-command!
     state
     (stamp-lease-command
      {:op :try-acquire-lease
       :req {:db-identity db-identity
             :leader-node-id leader-node-id
             :leader-endpoint leader-endpoint
             :lease-renew-ms lease-renew-ms
             :lease-timeout-ms lease-timeout-ms
             :leader-last-applied-lsn leader-last-applied-lsn
             :now-ms now-ms
             :observed-version observed-version
             :observed-lease observed-lease}}
      clock-skew-budget-ms)))

  (renew-lease [_ {:keys [db-identity leader-node-id leader-endpoint
                          term lease-renew-ms lease-timeout-ms
                          leader-last-applied-lsn now-ms]}]
    (ensure-running! running-v)
    (validate-renew-request!
      {:db-identity db-identity
       :leader-node-id leader-node-id
       :leader-endpoint leader-endpoint
       :term term
       :lease-renew-ms lease-renew-ms
       :lease-timeout-ms lease-timeout-ms
       :now-ms now-ms})
    (apply-state-command!
     state
     (stamp-lease-command
      {:op :renew-lease
       :req {:db-identity db-identity
             :leader-node-id leader-node-id
             :leader-endpoint leader-endpoint
             :term term
             :lease-renew-ms lease-renew-ms
             :lease-timeout-ms lease-timeout-ms
             :leader-last-applied-lsn leader-last-applied-lsn
             :now-ms now-ms}}
      clock-skew-budget-ms)))

  (release-lease [_ {:keys [db-identity leader-node-id term] :as req}]
    (ensure-running! running-v)
    (validate-release-request! req)
    (apply-state-command!
     state
     {:op :release-lease
      :req {:db-identity db-identity
            :leader-node-id leader-node-id
            :term term}}))

  (read-membership-hash [_]
    (ensure-running! running-v)
    (:membership-hash (in-memory-linearizable-state-snapshot! state)))

  (init-membership-hash! [_ membership-hash]
    (ensure-running! running-v)
    (require-non-blank-string! membership-hash :membership-hash)
    (lease/validate-membership-hash-key! group-id)
    (commit-state-transition!
     state
     (fn [s]
       (let [existing (:membership-hash s)]
         (cond
           (nil? existing)
           {:state (assoc s :membership-hash membership-hash)
            :result {:ok? true
                     :initialized? true
                     :membership-hash membership-hash}}

           (= existing membership-hash)
           {:state s
            :result {:ok? true
                     :initialized? false
                     :membership-hash existing}}

           :else
           {:state s
            :result {:ok? false
                     :reason :membership-hash-mismatch
                     :membership-hash existing
                     :expected membership-hash}})))))

  (read-voters [_]
    (ensure-running! running-v)
    (vec (:voters (in-memory-linearizable-state-snapshot! state))))

  (replace-voters! [_ voters]
    (ensure-running! running-v)
    (let [peer-ids (validated-peer-ids! voters :ha-control-plane-voters)]
      (swap! state assoc :voters peer-ids)
      {:ok? true
       :voters peer-ids})))

(defn- apply-try-acquire-transition
  [state {:keys [db-identity leader-node-id leader-endpoint
                 lease-renew-ms lease-timeout-ms
                 leader-last-applied-lsn now-ms
                 observed-version observed-lease] :as req}
   authority-now-ms
   clock-skew-budget-ms]
  (validate-acquire-request! req)
  (let [authority-now-ms (require-authority-now-ms authority-now-ms)
        skew-result (authority-clock-skew-result
                     now-ms
                     authority-now-ms
                     clock-skew-budget-ms)
        effective-now-ms authority-now-ms
        {:keys [lease version]} (lease-entry state db-identity)
        observed-version (long (or observed-version 0))
        current-version (long version)]
    (cond
      skew-result
      {:state state
       :result (assoc skew-result
                      :authority-now-ms authority-now-ms
                      :lease lease
                      :version current-version)}

      (not= observed-version current-version)
      {:state state
       :result {:ok? false
                :reason :cas-mismatch
                :authority-now-ms authority-now-ms
                :lease lease
                :version current-version}}

      (and (some? observed-lease) (not= observed-lease lease))
      {:state state
       :result {:ok? false
                :reason :observed-lease-mismatch
                :authority-now-ms authority-now-ms
                :lease lease
                :version current-version}}

      (and lease (not= db-identity (:db-identity lease)))
      {:state state
       :result {:ok? false
                :reason :db-identity-mismatch
                :authority-now-ms authority-now-ms
                :lease lease
                :version current-version}}

      (and lease (not (lease/lease-expired? lease effective-now-ms)))
      {:state state
       :result {:ok? false
                :reason :lease-not-expired
                :authority-now-ms authority-now-ms
                :lease lease
                :version current-version}}

      :else
      (let [observed    (or observed-lease lease)
            new-term    (lease/next-term observed)
            new-lease   (lease/new-lease-record
                         {:db-identity db-identity
                          :leader-node-id leader-node-id
                          :leader-endpoint leader-endpoint
                          :term new-term
                          :lease-renew-ms lease-renew-ms
                          :lease-timeout-ms lease-timeout-ms
                          :now-ms effective-now-ms
                          :leader-last-applied-lsn leader-last-applied-lsn})
            new-version (inc current-version)]
        {:state (assoc-in state [:leases db-identity]
                          {:lease new-lease
                           :version new-version})
         :result {:ok? true
                  :lease new-lease
                  :version new-version
                  :term new-term
                  :authority-now-ms authority-now-ms}}))))

(defn- apply-renew-transition
  [state {:keys [db-identity leader-node-id leader-endpoint
                 term lease-renew-ms lease-timeout-ms
                 leader-last-applied-lsn now-ms] :as req}
   authority-now-ms
   clock-skew-budget-ms]
  (validate-renew-request! req)
  (let [authority-now-ms (require-authority-now-ms authority-now-ms)
        skew-result (authority-clock-skew-result
                     now-ms
                     authority-now-ms
                     clock-skew-budget-ms)
        effective-now-ms authority-now-ms
        {:keys [lease version]} (lease-entry state db-identity)
        current-version (long version)]
    (cond
      skew-result
      {:state state
       :result (assoc skew-result
                      :authority-now-ms authority-now-ms
                      :lease lease
                      :version current-version)}

      (nil? lease)
      {:state state
       :result {:ok? false
                :reason :missing-lease
                :authority-now-ms authority-now-ms
                :version current-version}}

      (not= db-identity (:db-identity lease))
      {:state state
       :result {:ok? false
                :reason :db-identity-mismatch
                :authority-now-ms authority-now-ms
                :lease lease
                :version current-version}}

      (lease/lease-expired? lease effective-now-ms)
      {:state state
       :result {:ok? false
                :reason :lease-expired
                :authority-now-ms authority-now-ms
                :lease lease
                :version current-version}}

      (not= leader-node-id (:leader-node-id lease))
      {:state state
       :result {:ok? false
                :reason :owner-mismatch
                :authority-now-ms authority-now-ms
                :lease lease
                :version current-version}}

      (not= term (:term lease))
      {:state state
       :result {:ok? false
                :reason :term-mismatch
                :authority-now-ms authority-now-ms
                :lease lease
                :version current-version}}

      :else
      (let [new-lease   (lease/new-lease-record
                         {:db-identity db-identity
                          :leader-node-id leader-node-id
                          :leader-endpoint leader-endpoint
                          :term term
                          :lease-renew-ms lease-renew-ms
                          :lease-timeout-ms lease-timeout-ms
                          :now-ms effective-now-ms
                          :leader-last-applied-lsn leader-last-applied-lsn})
            new-version (inc current-version)]
        {:state (assoc-in state [:leases db-identity]
                          {:lease new-lease
                           :version new-version})
         :result {:ok? true
                  :lease new-lease
                  :version new-version
                  :term term
                  :authority-now-ms authority-now-ms}}))))

(defn- apply-release-transition
  [state {:keys [db-identity leader-node-id term] :as req}]
  (validate-release-request! req)
  (let [{:keys [lease version]} (lease-entry state db-identity)
        current-version (long version)]
    (cond
      (nil? lease)
      {:state state
       :result {:ok? true
                :released? false
                :reason :missing-lease
                :version current-version}}

      (not= db-identity (:db-identity lease))
      {:state state
       :result {:ok? false
                :reason :db-identity-mismatch
                :lease lease
                :version current-version}}

      (not= leader-node-id (:leader-node-id lease))
      {:state state
       :result {:ok? false
                :reason :owner-mismatch
                :lease lease
                :version current-version}}

      (not= term (:term lease))
      {:state state
       :result {:ok? false
                :reason :term-mismatch
                :lease lease
                :version current-version}}

      :else
      (let [new-version (inc current-version)]
        {:state (assoc-in state [:leases db-identity]
                          {:lease nil
                           :version new-version})
         :result {:ok? true
                  :released? true
                  :lease nil
                  :version new-version}}))))

(defn- apply-init-membership-hash-transition
  [state membership-hash]
  (require-non-blank-string! membership-hash :membership-hash)
  (let [existing (:membership-hash state)]
    (cond
      (nil? existing)
      {:state (assoc state :membership-hash membership-hash)
       :result {:ok? true
                :initialized? true
                :membership-hash membership-hash}}

      (= existing membership-hash)
      {:state state
       :result {:ok? true
                :initialized? false
                :membership-hash existing}}

      :else
      {:state state
       :result {:ok? false
                :reason :membership-hash-mismatch
                :membership-hash existing
                :expected membership-hash}})))

(defn- apply-read-state-transition
  [state db-identity]
  (let [{:keys [lease version]} (lease-entry state db-identity)]
    {:state state
     :result {:lease lease
              :version version
              :authority-now-ms (long (control-now-ms))
              :membership-hash (:membership-hash state)
              :voters (:voters state)}}))

(defn- attach-state-snapshot-to-result
  [{:keys [state result] :as transition}]
  (if (map? result)
    (assoc transition
           :result (assoc result
                          :membership-hash (:membership-hash state)
                          :voters (:voters state)))
    transition))

(defn- apply-state-command
  [state {:keys [op authority-now-ms clock-skew-budget-ms] :as cmd}]
  ;; Lease transitions consume the leader-stamped :authority-now-ms carried in
  ;; the replicated command so every peer applies the same authoritative lease
  ;; state. This is why the lease start time cannot be moved to local FSM apply.
  (case op
    :try-acquire-lease   (attach-state-snapshot-to-result
                          (apply-try-acquire-transition
                           state
                           (:req cmd)
                           authority-now-ms
                           clock-skew-budget-ms))
    :renew-lease         (attach-state-snapshot-to-result
                          (apply-renew-transition
                           state
                           (:req cmd)
                           authority-now-ms
                           clock-skew-budget-ms))
    :release-lease       (attach-state-snapshot-to-result
                          (apply-release-transition
                           state
                           (:req cmd)))
    :init-membership-hash (apply-init-membership-hash-transition
                           state (:membership-hash cmd))
    :read-state          (apply-read-state-transition state (:db-identity cmd))
    (u/raise "Unsupported HA control command"
             {:error :ha/control-invalid-command
              :command cmd})))

(defn- apply-state-command!
  [state-atom cmd]
  (commit-state-transition!
   state-atom
   #(apply-state-command % cmd)))

(defonce ^:private protobuf-loaded?
  (delay (do (ProtobufMsgFactory/load) true)))

(def ^:private forward-interest
  (.getName RpcRequests$ErrorResponse))

(def ^:private forward-request-code 9201)
(def ^:private forward-response-code 9202)
(def ^:private forward-request-tag :dtlv-ha-forward-v1)
(def ^:private default-rpc-timeout-ms 2000)
(def ^:private default-election-timeout-ms 3000)
(def ^:private default-operation-timeout-ms 5000)
(def ^:private default-snapshot-interval-secs 300)
(def ^:private max-read-index-attempt-timeout-ms 500)
(def ^:private max-command-attempt-timeout-ms 5000)
(def ^:private initial-command-retry-delay-ms 20)
(def ^:private read-retryable-errors
  #{RaftError/EAGAIN
    RaftError/EBUSY
    RaftError/EPERM
    RaftError/ETIMEDOUT
    RaftError/ERAFTTIMEDOUT})

(defn- retryable-read-status?
  [^Status status]
  (let [message (some-> status .getErrorMsg)]
    (or (contains? read-retryable-errors
                   (.getRaftError status))
        (and (string? message)
             (or (s/includes? message "leader stepped down")
                 (s/includes? message
                              "leader has not committed any log entry at its term")
                 (s/includes? message
                              "current node's apply index between leader's commit index over maxReadIndexLag"))))))

(defn- read-index-attempt-timeout-ms
  [remaining]
  (long (max 1
             (min (long remaining)
                  (long max-read-index-attempt-timeout-ms)))))

(defn- command-attempt-timeout-ms
  [remaining rpc-timeout-ms]
  (long (max 1
             (min (long remaining)
                  (long (or rpc-timeout-ms max-command-attempt-timeout-ms))
                  (long max-command-attempt-timeout-ms)))))

(defn- command-retry-delay-ms ^long
  [attempt remaining rpc-timeout-ms]
  (let [attempt      (max 0 (int attempt))
        cap-ms       (long (max 1
                                (min (long remaining)
                                     (long (or rpc-timeout-ms
                                               max-command-attempt-timeout-ms)))))
        delay-ms     (loop [delay-ms (long initial-command-retry-delay-ms)
                            attempt  attempt]
                   (if (or (zero? attempt)
                           (>= delay-ms cap-ms))
                     delay-ms
                     (let [doubled-delay (* 2 delay-ms)]
                       (recur (if (< doubled-delay cap-ms)
                                doubled-delay
                                cap-ms)
                              (dec attempt)))))]
    (if (> (long delay-ms) cap-ms)
      cap-ms
      (long delay-ms))))

(defn- ^:redef sleep-command-retry!
  [attempt remaining rpc-timeout-ms]
  (Thread/sleep (long (command-retry-delay-ms attempt remaining rpc-timeout-ms))))

(defn- command-operation-timeout-ms
  [operation-timeout-ms timeout-ms]
  (when (some? timeout-ms)
    (require-positive-int! timeout-ms :timeout-ms))
  (long (max 1
             (min (long operation-timeout-ms)
                  (long (or timeout-ms operation-timeout-ms))))))

(defn- single-voter-authority?
  [{:keys [voters]}]
  (= 1 (count voters)))

(defn- sanitize-path-segment
  [x]
  (-> x
      (str)
      (s/replace #"[^A-Za-z0-9._-]" "_")))

(defn- default-raft-dir
  [group-id local-peer-id]
  (u/tmp-dir (str "datalevin-ha-control/"
                  (sanitize-path-segment group-id)
                  "/"
                  (sanitize-path-segment local-peer-id))))

(defn- path-join
  [^String root ^String child]
  (.getAbsolutePath (File. root child)))

(declare parse-peer-id!)

(def ^:private fsm-snapshot-file
  "Serialized FSM state filename inside a JRaft snapshot directory."
  "fsm-state.nippy")

(defn- validated-peer-ids!
  [voters where]
  (when-not (vector? voters)
    (u/raise "HA control-plane voters must be a vector"
             {:error :ha/control-invalid-voters
              :where where
              :voters voters}))
  (when (empty? voters)
    (u/raise "HA control-plane voters cannot be empty"
             {:error :ha/control-invalid-voters
              :where where
              :voters voters}))
  (let [peer-ids
        (mapv (fn [idx v]
                (when-not (map? v)
                  (u/raise "HA control-plane voter must be a map"
                           {:error :ha/control-invalid-voters
                            :where [where idx]
                            :voter v}))
                (let [peer-id (:peer-id v)]
                  (require-non-blank-string! peer-id [where idx :peer-id])
                  (parse-peer-id! peer-id [where idx :peer-id])
                  peer-id))
              (range (count voters))
              voters)]
    (when (not= (count peer-ids) (count (distinct peer-ids)))
      (u/raise "HA control-plane voter peer IDs must be unique"
               {:error :ha/control-invalid-voters
                :where where
                :peer-ids peer-ids}))
    peer-ids))

(defn- parse-peer-id!
  [peer-id where]
  (let [p (PeerId.)]
    (when-not (.parse p peer-id)
      (u/raise "Invalid HA control peer-id"
               {:error :ha/control-invalid-peer-id
                :where where
                :peer-id peer-id}))
    p))

(defn- normalize-snapshot-state!
  [state]
  (when-not (map? state)
    (u/raise "HA control snapshot payload must be a map"
             {:error :ha/control-invalid-snapshot-state
              :state state}))
  (let [leases          (:leases state)
        membership-hash (:membership-hash state)
        voters          (or (:voters state) [])]
    (when-not (map? leases)
      (u/raise "HA control snapshot :leases must be a map"
               {:error :ha/control-invalid-snapshot-state
                :leases leases}))
    (when-not (or (nil? membership-hash)
                  (non-blank-string? membership-hash))
      (u/raise "HA control snapshot :membership-hash must be nil or non-blank string"
               {:error :ha/control-invalid-snapshot-state
                :membership-hash membership-hash}))
    (when-not (vector? voters)
      (u/raise "HA control snapshot :voters must be a vector"
               {:error :ha/control-invalid-snapshot-state
                :voters voters}))
    (let [peer-ids (mapv (fn [idx peer-id]
                           (require-non-blank-string!
                            peer-id [:snapshot :voters idx :peer-id])
                           (parse-peer-id!
                            peer-id [:snapshot :voters idx :peer-id])
                           peer-id)
                         (range (count voters))
                         voters)]
      (when (not= (count peer-ids) (count (distinct peer-ids)))
        (u/raise "HA control snapshot voter peer IDs must be unique"
                 {:error :ha/control-invalid-snapshot-state
                  :voters voters}))
      {:leases leases
       :membership-hash membership-hash
       :voters (vec (sort peer-ids))})))

(defn ^:redef atomic-move-replace-existing-paths!
  [from-path to-path]
  (Files/move from-path
              to-path
              ^"[Ljava.nio.file.CopyOption;"
              (into-array java.nio.file.CopyOption
                          [StandardCopyOption/REPLACE_EXISTING
                           StandardCopyOption/ATOMIC_MOVE])))

(defn- move-replace-existing!
  [^String from ^String to]
  (let [from-path (Paths/get from (make-array String 0))
        to-path   (Paths/get to (make-array String 0))]
    (try
      (atomic-move-replace-existing-paths! from-path to-path)
      (catch AtomicMoveNotSupportedException e
        (u/raise "HA control snapshot save requires atomic file replacement"
                 e
                 {:error :ha/control-snapshot-atomic-move-unsupported
                  :from from
                  :to to})))))

(defn- snapshot-state-file
  [snapshot-root]
  (path-join snapshot-root fsm-snapshot-file))

(defn- save-fsm-snapshot!
  [state-atom ^SnapshotWriter writer]
  (let [snapshot-root (.getPath writer)
        snapshot-path (snapshot-state-file snapshot-root)
        tmp-path      (str snapshot-path ".tmp")
        state         (normalize-snapshot-state!
                        (merge (blank-state) @state-atom))]
    (u/create-dirs snapshot-root)
    (u/dump-bytes tmp-path ^bytes (nippy/freeze state))
    (move-replace-existing! tmp-path snapshot-path)
    (when-not (.addFile writer fsm-snapshot-file)
      (u/raise "Failed to add HA control FSM snapshot file"
               {:error :ha/control-snapshot-save-failed
                :snapshot-file fsm-snapshot-file
                :snapshot-root snapshot-root}))))

(defn- load-fsm-snapshot!
  [state-atom ^SnapshotReader reader]
  (let [snapshot-root (.getPath reader)
        files         (set (.listFiles reader))]
    (when-not (contains? files fsm-snapshot-file)
      (u/raise "HA control FSM snapshot file is missing"
               {:error :ha/control-snapshot-load-failed
                :snapshot-file fsm-snapshot-file
                :snapshot-root snapshot-root
                :files files}))
    (let [snapshot-path (snapshot-state-file snapshot-root)
          state         (-> (Files/readAllBytes
                             (Paths/get snapshot-path (make-array String 0)))
                            nippy/thaw
                            normalize-snapshot-state!)]
      (reset! state-atom state)
      true)))

(defn- leader-peer-id
  [^Node node]
  (let [p (.getLeaderId node)]
    (when (and p (not (.isEmpty p)))
      p)))

(defn- peer-id-string
  [^PeerId p]
    (when (and p (not (.isEmpty p)))
      (.toString p)))

(defn- peer-ids->configuration
  [peer-ids where]
  (let [peers (mapv #(parse-peer-id! % where) peer-ids)]
    (doto (Configuration.)
      (.setPeers peers))))

(defn- node-peer-ids
  [^Node node]
  (->> (.listPeers node)
       (map peer-id-string)
       (remove nil?)
       sort
       vec))

(defn- safe-node-value
  [f]
  (try
    (f)
    (catch Exception e
      {:error (ex-message e)
       :class (some-> e class .getName)})))

(defn- configuration-peer-ids
  [^Configuration conf]
  (->> (.listPeers conf)
       (map peer-id-string)
       (remove nil?)
       sort
       vec))

(defn- bytebuffer->bytes
  [^ByteBuffer bb]
  (let [buf (.duplicate bb)
        n   (.remaining buf)
        out (byte-array n)]
    (.get buf out)
    out))

(defn- freeze->base64
  [x]
  (.encodeToString (Base64/getUrlEncoder)
                   ^bytes (nippy/freeze x)))

(defn- thaw-from-base64
  [^String s]
  (nippy/thaw (.decode (Base64/getUrlDecoder) s)))

(defn- control-message
  [code payload]
  (.build
    (doto (RpcRequests$ErrorResponse/newBuilder)
      (.setErrorCode (int code))
      (.setErrorMsg (freeze->base64 payload)))))

(defn- control-payload
  [^RpcRequests$ErrorResponse msg]
  (thaw-from-base64 (.getErrorMsg msg)))

(defn- status-data
  [^Status status]
  {:code (.getCode status)
   :raft-error (some-> status .getRaftError str)
   :message (.getErrorMsg status)})

(defrecord CommandClosure [result-v result-p]
  Closure
  (run [_ status]
    (deliver result-p
             {:status status
              :result @result-v})))

(defrecord StatusClosure [status-p]
  Closure
  (run [_ status]
    (deliver status-p status)))

(defn- run-command-closure!
  ([^CommandClosure done ^Status status]
   (.run done status))
  ([^CommandClosure done result ^Status status]
   (vreset! (:result-v done) result)
   (.run done status)))

(defn- fsm-apply-error-status
  [^Exception e]
  (Status.
   (int (.getNumber RaftError/ESTATEMACHINE))
   (str "HA FSM apply failed: "
        (.getMessage e))))

(defn- command->byte-buffer
  [cmd]
  (ByteBuffer/wrap ^bytes (nippy/freeze cmd)))

(defn- ^:redef apply-local-command-once!
  [^Node node cmd timeout-ms]
  (let [result-p (promise)
        closure  (->CommandClosure (volatile! nil) result-p)
        task     (doto (Task.)
                   (.setData (command->byte-buffer cmd))
                   (.setDone closure))]
    (.apply node task)
    (let [outcome (deref result-p (long timeout-ms) ::timeout)]
      (if (= ::timeout outcome)
        {:ok? false
         :error :timeout}
        (let [^Status status (:status outcome)]
          (cond
            (nil? status)
            {:ok? false :error :missing-status}

            (.isOk status)
            {:ok? true :result (:result outcome)}

            (contains? #{RaftError/EPERM RaftError/EBUSY RaftError/EAGAIN}
                       (.getRaftError status))
            {:ok? false
             :error :not-leader
             :status status}

            :else
            {:ok? false
             :error :apply-failed
             :status status}))))))

(defn- change-peers-once!
  [^Node node peer-ids timeout-ms]
  (let [status-p (promise)
        closure  (->StatusClosure status-p)
        conf     (peer-ids->configuration peer-ids :ha-control-plane-voters)]
    (.changePeers node conf closure)
    (let [outcome (deref status-p (long timeout-ms) ::timeout)]
      (if (= ::timeout outcome)
        {:ok? false
         :error :timeout}
        (let [^Status status outcome]
          (cond
            (nil? status)
            {:ok? false :error :missing-status}

            (.isOk status)
            {:ok? true}

            (contains? #{RaftError/EPERM RaftError/EBUSY RaftError/EAGAIN}
                       (.getRaftError status))
            {:ok? false
             :error :not-leader
             :status status}

            :else
            {:ok? false
             :error :change-peers-failed
             :status status}))))))

(defn- running-node!
  [{:keys [node-v]}]
  (if-let [^Node node @node-v]
    node
    (u/raise "HA control-plane node is unavailable"
             {:error :ha/control-node-unavailable})))

(defn- running-runtime!
  [{:keys [node-v rpc-client-v]}]
  (let [^Node node @node-v
        ^RpcClient rpc-client @rpc-client-v]
    (when-not node
      (u/raise "HA control-plane node is unavailable"
               {:error :ha/control-node-unavailable}))
    (when-not rpc-client
      (u/raise "HA control-plane rpc client is unavailable"
               {:error :ha/control-rpc-unavailable}))
    {:node node
     :rpc-client rpc-client}))

(defn- root-cause
  [^Throwable e]
  (loop [t e]
    (if-let [cause (some-> t .getCause)]
      (recur cause)
      t)))

(defn- forward-connect-failure?
  [^Throwable e]
  (let [cause (root-cause e)]
    (or (instance? ConnectException cause)
        (instance? NoRouteToHostException cause))))

(defn- plain-socket-connect-diagnostics
  [^PeerId leader timeout-ms]
  (let [endpoint (.getEndpoint leader)
        host     (.getIp endpoint)
        port     (.getPort endpoint)
        timeout  (int (max 1 (long timeout-ms)))]
    (try
      (with-open [socket (Socket.)]
        (.connect socket (InetSocketAddress. host port) timeout)
        {:ok? true
         :host host
         :port port
         :local-socket (some-> socket .getLocalSocketAddress str)
         :remote-socket (some-> socket .getRemoteSocketAddress str)})
      (catch Throwable t
        {:ok? false
         :host host
         :port port
         :error-class (.getName (class t))
         :message (.getMessage t)}))))

(defn- fresh-rpc-client
  [rpc-timeout-ms]
  (let [timeout-ms (long (or rpc-timeout-ms
                             max-command-attempt-timeout-ms))
        ^RpcClient client (.createRpcClient (RpcFactoryHelper/rpcFactory))
        ^RpcOptions opts (doto (RpcOptions.)
                           (.setRpcConnectTimeoutMs (int timeout-ms))
                           (.setRpcDefaultTimeout (int timeout-ms)))]
    (when-not (.init client opts)
      (u/raise "Failed to initialize HA control rpc client"
               {:error :ha/control-rpc-init-failed}))
    client))

(defn- invoke-forward-with-fresh-rpc-client
  [rpc-timeout-ms ^PeerId leader request invoke-timeout]
  (let [^RpcClient fresh-client (fresh-rpc-client rpc-timeout-ms)]
    (try
      (.invokeSync fresh-client
                   (.getEndpoint leader)
                   request
                   invoke-timeout)
      (finally
        (try
          (.shutdown fresh-client)
          (catch Exception shutdown-e
            (log/warn shutdown-e "Failed to stop temporary HA control rpc client"
                      {:leader (peer-id-string leader)})))))))

(defn- invoke-forward-request
  [^RpcClient rpc-client rpc-timeout-ms ^PeerId leader request
   invoke-timeout attempt]
  (let [leader-str (peer-id-string leader)]
    (try
      (.invokeSync rpc-client
                   (.getEndpoint leader)
                   request
                   invoke-timeout)
      (catch InterruptedException e
        (.interrupt (Thread/currentThread))
        (u/raise "HA control forward interrupted"
                 {:error :ha/control-interrupted
                  :attempt attempt}))
      (catch Exception e
        (if (forward-connect-failure? e)
          (let [plain-socket (plain-socket-connect-diagnostics
                              leader invoke-timeout)]
            (log/warn e "HA control forward failed with cached rpc client; retrying with fresh client"
                      {:attempt attempt
                       :leader leader-str
                       :plain-socket plain-socket})
            (try
              (invoke-forward-with-fresh-rpc-client
               rpc-timeout-ms leader request invoke-timeout)
              (catch InterruptedException fresh-e
                (.interrupt (Thread/currentThread))
                (u/raise "HA control forward interrupted"
                         {:error :ha/control-interrupted
                          :attempt attempt}))
              (catch Exception fresh-e
                (let [plain-socket (plain-socket-connect-diagnostics
                                    leader invoke-timeout)]
                  (log/warn fresh-e "HA control forward with fresh rpc client failed"
                            {:attempt attempt
                             :leader leader-str
                             :plain-socket plain-socket})
                  ::invoke-failed))))
          (do
            (log/warn e "HA control forward failed"
                      {:attempt attempt
                       :leader leader-str})
            ::invoke-failed))))))

(defn- authority-fsm-snapshot
  [{:keys [fsm-state]}]
  (some-> fsm-state deref))

(declare authority-diagnostics submit-command!)

(defn- await-linearizable-read!
  [{:keys [operation-timeout-ms] :as authority}]
  (let [deadline (+ (System/currentTimeMillis) (long operation-timeout-ms))]
    (loop [attempt 0]
      (let [remaining (- deadline (System/currentTimeMillis))
            ^Node node (running-node! authority)]
        (if (<= remaining 0)
          (u/raise "HA control readIndex timed out"
                   {:error :ha/control-timeout
                    :where :read-index
                    :attempt attempt
                    :leader? (.isLeader node)
                    :authority (authority-diagnostics authority)})
          (let [status-p   (promise)
                timeout-ms (read-index-attempt-timeout-ms remaining)
                invoked?   (try
                             (.readIndex node (byte-array 0)
                                         (proxy [ReadIndexClosure] []
                                           (run
                                             ([^Status status]
                                              (deliver status-p
                                                       {:status status
                                                        :snapshot
                                                        (authority-fsm-snapshot
                                                         authority)}))
                                             ([^Status status _index _request-ctx]
                                              (deliver status-p
                                                       {:status status
                                                        :snapshot
                                                        (authority-fsm-snapshot
                                                         authority)})))))
                             true
                             (catch Exception e
                               (log/warn e "HA control readIndex invocation failed")
                               false))]
            (if-not invoked?
              (do (Thread/sleep 20)
                  (recur (inc attempt)))
              (let [result (deref status-p timeout-ms ::timeout)
                    status (:status result)]
                (cond
                  (= ::timeout result)
                  (do (Thread/sleep 20)
                      (recur (inc attempt)))

                  (.isOk ^Status status)
                  (:snapshot result)

                  (retryable-read-status? ^Status status)
                  (do (Thread/sleep 20)
                      (recur (inc attempt)))

                  :else
                  (u/raise "HA control readIndex failed"
                           {:error :ha/control-read-failed
                            :status (status-data ^Status status)
                            :authority (authority-diagnostics authority)}))))))))))

(defn ^:redef await-read-state-barrier!
  [authority]
  (await-linearizable-read! authority))

(defn ^:redef linearizable-read-snapshot!
  [authority]
  (await-linearizable-read! authority))

(defn ^:redef submit-read-state-command!
  [authority db-identity]
  (submit-command! authority {:op :read-state
                              :db-identity db-identity}))

(defn- forward-request-processor
  [authority]
  (reify RpcProcessor
    (^void handleRequest [_ ^RpcContext rpc-context req]
      (let [response
            (try
              (let [^RpcRequests$ErrorResponse req-msg req]
                (if (not= forward-request-code (.getErrorCode req-msg))
                  (control-message forward-response-code
                                   {:ok? false
                                    :error :unsupported-forward-code
                                    :code (.getErrorCode req-msg)})
                  (let [payload (control-payload req-msg)]
                    (if (not= forward-request-tag (:tag payload))
                      (control-message forward-response-code
                                       {:ok? false
                                        :error :invalid-forward-tag})
                      (let [^Node node (running-node! authority)]
                        (if-not (.isLeader node)
                          (control-message forward-response-code
                                           {:ok? false
                                            :error :not-leader
                                            :leader-peer-id
                                            (peer-id-string (leader-peer-id node))})
                          (let [op (or (:op payload)
                                       (when (contains? payload :command)
                                         :apply-command))]
                            (case op
                              :apply-command
                              (let [cmd (authoritative-command
                                         authority
                                         (:command payload))
                                    res (apply-local-command-once!
                                         node cmd
                                         (:operation-timeout-ms authority))]
                                (cond
                                  (:ok? res)
                                  (control-message forward-response-code
                                                   {:ok? true
                                                    :result (:result res)})

                                  (= :not-leader (:error res))
                                  (control-message forward-response-code
                                                   {:ok? false
                                                    :error :not-leader
                                                    :leader-peer-id
                                                    (peer-id-string
                                                      (leader-peer-id node))
                                                    :status (some-> (:status res)
                                                                    status-data)})

                                  (= :timeout (:error res))
                                  (control-message forward-response-code
                                                   {:ok? false
                                                    :error :apply-timeout})

                                  :else
                                  (control-message forward-response-code
                                                   {:ok? false
                                                    :error :apply-failed
                                                    :status (some-> (:status res)
                                                                    status-data)})))

                              :change-peers
                              (let [peer-ids (validated-peer-ids!
                                              (:voters payload)
                                              :ha-control-plane-voters)
                                    res      (change-peers-once!
                                              node
                                              peer-ids
                                              (:operation-timeout-ms authority))]
                                (cond
                                  (:ok? res)
                                  (control-message forward-response-code
                                                   {:ok? true
                                                    :voters (node-peer-ids node)})

                                  (= :not-leader (:error res))
                                  (control-message forward-response-code
                                                   {:ok? false
                                                    :error :not-leader
                                                    :leader-peer-id
                                                    (peer-id-string
                                                      (leader-peer-id node))
                                                    :status (some-> (:status res)
                                                                    status-data)})

                                  (= :timeout (:error res))
                                  (control-message forward-response-code
                                                   {:ok? false
                                                    :error :change-peers-timeout})

                                  :else
                                  (control-message forward-response-code
                                                   {:ok? false
                                                    :error :change-peers-failed
                                                    :status (some-> (:status res)
                                                                    status-data)})))

                              (control-message forward-response-code
                                               {:ok? false
                                                :error :unsupported-forward-op
                                                :op op})))))))))
              (catch Exception e
                (log/warn e "HA control forward processor failed")
                (control-message forward-response-code
                                 {:ok? false
                                  :error :forward-processor-failed
                                  :message (.getMessage e)})))]
        (.sendResponse rpc-context response)))

    (interest [_]
      forward-interest)))

(defn- submit-command!
  [{:keys [rpc-timeout-ms operation-timeout-ms] :as authority}
   {:keys [timeout-ms] :as cmd}]
  (let [start-ms (long (System/currentTimeMillis))
        deadline (+ start-ms
                    (long (command-operation-timeout-ms
                           operation-timeout-ms
                           timeout-ms)))]
    (loop [attempt 0
           now-ms  start-ms]
      (let [remaining (- deadline now-ms)]
        (when (<= remaining 0)
          (u/raise "HA control command timed out"
                   {:error :ha/control-timeout
                    :attempt attempt
                    :command (:op cmd)}))
        (let [{:keys [^Node node ^RpcClient rpc-client]}
              (running-runtime! authority)]
          (if (.isLeader node)
            (let [cmd (authoritative-command authority cmd)
                  local-timeout (command-attempt-timeout-ms
                                 remaining
                                 rpc-timeout-ms)
                  local-res (apply-local-command-once!
                             node cmd local-timeout)]
              (cond
                (:ok? local-res)
                (:result local-res)

                (#{:not-leader :timeout} (:error local-res))
                (do (sleep-command-retry! attempt remaining rpc-timeout-ms)
                    (recur (inc attempt)
                           (long (System/currentTimeMillis))))

                :else
                (u/raise "HA control local apply failed"
                         {:error :ha/control-apply-failed
                          :attempt attempt
                          :command (:op cmd)
                          :status (some-> (:status local-res)
                                          status-data)})))
            (if-let [^PeerId leader (leader-peer-id node)]
              (let [request (control-message forward-request-code
                                             {:tag forward-request-tag
                                              :command cmd})
                    invoke-timeout
                    (let [rpc-timeout (long (or rpc-timeout-ms
                                                max-command-attempt-timeout-ms))
                          cap         (if (< remaining rpc-timeout)
                                        remaining
                                        rpc-timeout)]
                      (if (> cap 1) cap 1))
                    response (invoke-forward-request
                              rpc-client
                              rpc-timeout-ms
                              leader
                              request
                              invoke-timeout
                              attempt)]
                (cond
                  (= ::invoke-failed response)
                  (do (sleep-command-retry! attempt remaining rpc-timeout-ms)
                      (recur (inc attempt)
                             (long (System/currentTimeMillis))))

                  (not (instance? RpcRequests$ErrorResponse response))
                  (do (sleep-command-retry! attempt remaining rpc-timeout-ms)
                      (recur (inc attempt)
                             (long (System/currentTimeMillis))))

                  :else
                  (let [^RpcRequests$ErrorResponse response-msg response
                        payload (try
                                  (control-payload response-msg)
                                  (catch Exception e
                                    (log/warn e "HA control payload decode failed")
                                    {:ok? false
                                     :error :payload-decode-failed}))]
                    (cond
                      (not= forward-response-code
                            (.getErrorCode response-msg))
                      (do (sleep-command-retry! attempt remaining rpc-timeout-ms)
                          (recur (inc attempt)
                                 (long (System/currentTimeMillis))))

                      (:ok? payload)
                      (:result payload)

                      (contains? #{:not-leader :apply-timeout :node-unavailable}
                                 (:error payload))
                      (do (sleep-command-retry! attempt remaining rpc-timeout-ms)
                          (recur (inc attempt)
                                 (long (System/currentTimeMillis))))

                      :else
                      (u/raise "HA control forward response failed"
                               {:error :ha/control-forward-failed
                                :attempt attempt
                                :payload payload})))))
              (do (sleep-command-retry! attempt remaining rpc-timeout-ms)
                  (recur (inc attempt)
                         (long (System/currentTimeMillis)))))))))))

(defn- new-jraft-fsm
  [state-atom]
  (proxy [StateMachineAdapter] []
    (onApply [^Iterator iter]
      ;; Apply and commit each log entry in order so the authoritative state
      ;; and closure outcomes track JRaft's committed-prefix semantics.
      (loop [state @state-atom]
        (if (.hasNext iter)
          (let [^ByteBuffer data (.getData iter)
                done           (.done iter)
                step           (try
                                 (let [cmd (nippy/thaw (bytebuffer->bytes data))
                                       {:keys [state result]}
                                       (apply-state-command state cmd)]
                                   {:state state
                                    :result result})
                                 (catch Exception e
                                   {:error e}))]
            (if-let [e (:error step)]
              (let [status (fsm-apply-error-status e)]
                (log/error e "HA control JRaft FSM apply failed")
                (when (instance? CommandClosure done)
                  (run-command-closure! done status))
                (.setErrorAndRollback iter 1 status))
              (do
                (reset! state-atom (:state step))
                (.commit iter)
                (when (instance? CommandClosure done)
                  (run-command-closure! done
                                        (:result step)
                                        (Status/OK)))
                (.next iter)
                (recur (:state step)))))
          nil)))

    (onSnapshotSave [^SnapshotWriter writer done]
      (try
        (save-fsm-snapshot! state-atom writer)
        (when done
          (.run ^Closure done (Status/OK)))
        (catch Exception e
          (log/error e "HA control JRaft FSM snapshot save failed"
                     {:snapshot-path (some-> writer .getPath)})
          (when done
            (.run ^Closure done
                  (Status.
                   (int (.getNumber RaftError/EIO))
                   (str "HA FSM snapshot save failed: "
                        (.getMessage e))))))))

    (onSnapshotLoad [^SnapshotReader reader]
      (try
        (load-fsm-snapshot! state-atom reader)
        (catch Exception e
          (log/error e "HA control JRaft FSM snapshot load failed"
                     {:snapshot-path (some-> reader .getPath)})
          false)))

    (onConfigurationCommitted [^Configuration conf]
      (try
        (swap! state-atom assoc :voters (configuration-peer-ids conf))
        (catch Exception e
          (log/warn e "HA control JRaft configuration callback failed"
                    {:configuration (str conf)}))))))

(defrecord SofaJraftLeaseAuthority [group-id local-peer-id voters
                                    rpc-timeout-ms election-timeout-ms
                                    operation-timeout-ms raft-dir
                                    clock-skew-budget-ms
                                    fsm-state node-v group-service-v
                                    rpc-client-v running-v]
  ILeaseAuthority
  (start-authority! [this]
    (locking this
      (when-not (running? running-v)
        (let [group-service-box (volatile! nil)
              rpc-client-box    (volatile! nil)]
          (try
            @protobuf-loaded?
            (let [^PeerId local-peer (parse-peer-id! local-peer-id
                                                     :local-peer-id)
                  peer-ids           (validated-peer-ids!
                                      voters
                                      :ha-control-plane-voters)
                  conf               (peer-ids->configuration
                                      peer-ids
                                      :ha-control-plane-voters)
                  root-dir           (or raft-dir
                                         (default-raft-dir
                                           group-id local-peer-id))
                  log-dir            (path-join root-dir "log")
                  meta-dir           (path-join root-dir "meta")
                  snapshot-dir       (path-join root-dir "snapshot")
                  _                  (doseq [d [root-dir log-dir
                                                meta-dir snapshot-dir]]
                                       (u/create-dirs d))
                  _                  (reset! fsm-state
                                      (assoc (blank-state)
                                             :voters (vec (sort peer-ids))))
                  fsm                (new-jraft-fsm fsm-state)
                  ^NodeOptions opts  (doto (NodeOptions.)
                                       (.setFsm fsm)
                                       (.setServiceFactory
                                        LMDBJRaftServiceFactory/INSTANCE)
                                       (.setInitialConf conf)
                                       (.setElectionTimeoutMs
                                        (int election-timeout-ms))
                                       (.setRpcConnectTimeoutMs
                                        (int rpc-timeout-ms))
                                       (.setRpcDefaultTimeout
                                        (int rpc-timeout-ms))
                                       (.setLogUri log-dir)
                                       (.setRaftMetaUri meta-dir)
                                       (.setSnapshotUri snapshot-dir)
                                       (.setSnapshotIntervalSecs
                                        (int default-snapshot-interval-secs)))
                  service            (RaftGroupService.
                                      group-id local-peer opts)
                  node               (.start service)
                  client             (.createRpcClient
                                      (RpcFactoryHelper/rpcFactory))]
              (when-not node
                (u/raise "Failed to start HA control JRaft node"
                         {:error :ha/control-start-failed
                          :group-id group-id
                          :peer-id local-peer-id}))
              (when-not (.init ^RpcClient client opts)
                (u/raise "Failed to initialize HA control rpc client"
                         {:error :ha/control-rpc-init-failed
                          :group-id group-id
                          :peer-id local-peer-id}))
                  (.registerProcessor
                   (.getRpcServer service)
                   (forward-request-processor this))
                  (vreset! group-service-box service)
                  (vreset! rpc-client-box client)
                  (vreset! group-service-v service)
                  (vreset! node-v node)
                  (vreset! rpc-client-v client)
                  (vreset! running-v true)
                  (log/info "Started HA control JRaft authority"
                            {:group-id group-id
                             :peer-id local-peer-id
                             :voter-count (count voters)}))
            (catch Exception e
              (when-let [^RpcClient client @rpc-client-box]
                (try
                  (.shutdown client)
                  (catch Exception shutdown-e
                    (log/warn shutdown-e "Failed to stop HA control rpc client"))))
              (when-let [^RaftGroupService service @group-service-box]
                (try
                  (.shutdown service)
                  (catch Exception shutdown-e
                    (log/warn shutdown-e "Failed to shutdown HA control raft service")))
                (try
                  (.join service)
                  (catch Exception join-e
                    (log/warn join-e "Failed to join HA control raft service"))))
              (vreset! group-service-v nil)
              (vreset! node-v nil)
              (vreset! rpc-client-v nil)
              (vreset! running-v false)
              (throw e))))))
    this)

  (stop-authority! [this]
    (locking this
      (when (running? running-v)
        (vreset! running-v false)
        (when-let [^RpcClient client @rpc-client-v]
          (try
            (.shutdown client)
            (catch Exception e
              (log/warn e "Failed to stop HA control rpc client"))))
        (when-let [^RaftGroupService service @group-service-v]
          (try
            (.shutdown service)
            (catch Exception e
              (log/warn e "Failed to shutdown HA control raft service")))
          (try
            (.join service)
            (catch Exception e
              (log/warn e "Failed to join HA control raft service"))))
        (vreset! group-service-v nil)
        (vreset! node-v nil)
        (vreset! rpc-client-v nil)))
    this)

  (read-lease [this db-identity]
    (ensure-running! running-v)
    (require-non-blank-string! db-identity :db-identity)
    (lease/validate-lease-key! group-id db-identity)
    (let [snapshot (linearizable-read-snapshot! this)
          {:keys [lease version]} (lease-entry snapshot db-identity)]
      {:lease lease
       :version version}))

  (try-acquire-lease [this req]
    (ensure-running! running-v)
    (lease/validate-lease-key! group-id (:db-identity req))
    (submit-command! this {:op :try-acquire-lease
                           :req req}))

  (renew-lease [this req]
    (ensure-running! running-v)
    (lease/validate-lease-key! group-id (:db-identity req))
    (submit-command! this {:op :renew-lease
                           :timeout-ms (:timeout-ms req)
                           :req req}))

  (release-lease [this req]
    (ensure-running! running-v)
    (lease/validate-lease-key! group-id (:db-identity req))
    (submit-command! this {:op :release-lease
                           :req req}))

  (read-membership-hash [this]
    (ensure-running! running-v)
    (lease/validate-membership-hash-key! group-id)
    (:membership-hash (linearizable-read-snapshot! this)))

  (init-membership-hash! [this membership-hash]
    (ensure-running! running-v)
    (lease/validate-membership-hash-key! group-id)
    (submit-command! this {:op :init-membership-hash
                           :membership-hash membership-hash}))

  (read-voters [this]
    (ensure-running! running-v)
    (:voters (linearizable-read-snapshot! this)))

  (replace-voters! [this voters]
    (ensure-running! running-v)
    (let [peer-ids (validated-peer-ids! voters :ha-control-plane-voters)
          deadline (+ (System/currentTimeMillis) (long operation-timeout-ms))]
      (loop [attempt 0]
        (let [remaining (- deadline (System/currentTimeMillis))]
          (when (<= remaining 0)
            (u/raise "HA control voter reconfiguration timed out"
                     {:error :ha/control-timeout
                      :where :replace-voters
                      :attempt attempt
                      :peer-ids peer-ids}))
          (let [{:keys [^Node node ^RpcClient rpc-client]}
                (running-runtime! this)]
            (if (.isLeader node)
              (let [local-timeout (command-attempt-timeout-ms
                                   remaining
                                   rpc-timeout-ms)
                    local-res (change-peers-once!
                               node peer-ids local-timeout)]
                (cond
                  (:ok? local-res)
                  {:ok? true
                   :voters (node-peer-ids node)}

                  (#{:not-leader :timeout} (:error local-res))
                  (do (Thread/sleep 20)
                      (recur (inc attempt)))

                  :else
                  (u/raise "HA control voter reconfiguration failed"
                           {:error :ha/control-change-peers-failed
                            :attempt attempt
                            :peer-ids peer-ids
                            :status (some-> (:status local-res)
                                            status-data)})))
              (if-let [^PeerId leader (leader-peer-id node)]
                (let [request (control-message
                               forward-request-code
                               {:tag forward-request-tag
                                :op :change-peers
                                :voters voters})
                      invoke-timeout (long (max 1 (min remaining
                                                       (long rpc-timeout-ms))))
                      response (invoke-forward-request
                                rpc-client
                                rpc-timeout-ms
                                leader
                                request
                                invoke-timeout
                                attempt)]
                  (cond
                    (= ::invoke-failed response)
                    (do (Thread/sleep 20)
                        (recur (inc attempt)))

                    (not (instance? RpcRequests$ErrorResponse response))
                    (do (Thread/sleep 20)
                        (recur (inc attempt)))

                    :else
                    (let [^RpcRequests$ErrorResponse response-msg response
                          payload (try
                                    (control-payload response-msg)
                                    (catch Exception e
                                      (log/warn e "HA control payload decode failed")
                                      {:ok? false
                                       :error :payload-decode-failed}))]
                      (cond
                        (not= forward-response-code
                              (.getErrorCode response-msg))
                        (do (Thread/sleep 20)
                            (recur (inc attempt)))

                        (:ok? payload)
                        {:ok? true
                         :voters (:voters payload)}

                        (contains? #{:not-leader :change-peers-timeout
                                     :node-unavailable}
                                   (:error payload))
                        (do (Thread/sleep 20)
                            (recur (inc attempt)))

                        :else
                        (u/raise "HA control forward response failed"
                                 {:error :ha/control-forward-failed
                                  :attempt attempt
                                  :payload payload})))))
                (do (Thread/sleep 20)
                    (recur (inc attempt))))))))))

  )

(defn authority-diagnostics
  "Best-effort runtime snapshot for HA control authorities."
  [authority]
  (try
    (cond
      (instance? InMemoryLeaseAuthority authority)
      (let [{:keys [group-id state running-v initial-voters
                    clock-skew-budget-ms]} authority
            snapshot @state]
        {:backend :in-memory
         :group-id group-id
         :running? (running? running-v)
         :clock-skew-budget-ms clock-skew-budget-ms
         :initial-voters initial-voters
         :voters (:voters snapshot)
         :membership-hash (:membership-hash snapshot)
         :lease-count (count (:leases snapshot))})

      (instance? SofaJraftLeaseAuthority authority)
      (let [{:keys [group-id local-peer-id voters
                    rpc-timeout-ms election-timeout-ms
                    operation-timeout-ms clock-skew-budget-ms
                    fsm-state node-v
                    running-v]} authority
            snapshot @fsm-state
            ^Node node @node-v
            leader-id (when node
                        (safe-node-value
                          #(peer-id-string (leader-peer-id node))))
            peer-ids (when node
                       (safe-node-value
                         #(node-peer-ids node)))
            alive-peer-ids (when node
                             (safe-node-value
                               #(->> (.listAlivePeers node)
                                     (map peer-id-string)
                                     (remove nil?)
                                     sort
                                     vec)))]
        {:backend :sofa-jraft
         :group-id group-id
         :local-peer-id local-peer-id
         :running? (running? running-v)
         :configured-voters (mapv :peer-id voters)
         :rpc-timeout-ms rpc-timeout-ms
         :election-timeout-ms election-timeout-ms
         :operation-timeout-ms operation-timeout-ms
         :clock-skew-budget-ms clock-skew-budget-ms
         :fsm-voters (:voters snapshot)
         :fsm-membership-hash (:membership-hash snapshot)
         :fsm-lease-count (count (:leases snapshot))
         :node-available? (some? node)
         :node-leader? (when node (.isLeader node))
         :node-state (when node
                       (safe-node-value
                         #(some-> (.getNodeState node) str)))
         :last-log-index (when node (safe-node-value #(.getLastLogIndex node)))
         :last-committed-index (when node
                                 (safe-node-value
                                   #(.getLastCommittedIndex node)))
         :last-applied-log-index (when node
                                   (safe-node-value
                                     #(.getLastAppliedLogIndex node)))
         :leader-peer-id leader-id
         :node-peer-ids peer-ids
         :alive-peer-ids alive-peer-ids})

      :else
      {:backend :unknown
       :class (some-> authority class .getName)})
    (catch Exception e
      {:backend :diagnostics-failed
       :class (some-> authority class .getName)
       :message (ex-message e)})))

(defn read-state
  "Read the HA control snapshot for db-identity.

  For the SOFAJRaft backend this uses a linearizable readIndex barrier and then
  serves the snapshot from the local FSM state, avoiding a replicated command
  on every steady-state HA renew cycle. If readIndex itself times out, surface
  that failure to the caller instead of falling back to a replicated command.
  The startup path uses read-state-for-startup when it explicitly wants the
  slower command-based read."
  [authority db-identity]
  (require-non-blank-string! db-identity :db-identity)
  (cond
    (instance? InMemoryLeaseAuthority authority)
    (let [{:keys [group-id state running-v]} authority]
      (ensure-running! running-v)
      (lease/validate-lease-key! group-id db-identity)
    (let [snapshot (in-memory-linearizable-state-snapshot! state)
          {:keys [lease version]} (lease-entry snapshot db-identity)]
      {:lease lease
       :version version
       :authority-now-ms (long (control-now-ms))
       :membership-hash (:membership-hash snapshot)
       :voters (:voters snapshot)}))

    (instance? SofaJraftLeaseAuthority authority)
    (let [{:keys [group-id running-v]} authority]
      (ensure-running! running-v)
      (lease/validate-lease-key! group-id db-identity)
      (let [snapshot (await-read-state-barrier! authority)
            {:keys [lease version]} (lease-entry snapshot db-identity)]
        {:lease lease
         :version version
         :authority-now-ms (long (control-now-ms))
         :membership-hash (:membership-hash snapshot)
         :voters (:voters snapshot)}))

    (satisfies? ILeaseAuthority authority)
    (let [{:keys [lease version]} (read-lease authority db-identity)]
      {:lease lease
       :version version
       :membership-hash (read-membership-hash authority)
       :voters (read-voters authority)})

    :else
    (u/raise "Unsupported HA control authority type"
             {:error :ha/control-unsupported-authority
              :class (some-> authority class .getName)})))

(defn read-state-for-startup
  "Read the HA control snapshot for db-identity during startup.

  For the SOFAJRaft backend this preserves the replicated command path because
  the full DB-server HA startup path can stall in readIndex even when the raft
  group is otherwise healthy."
  [authority db-identity]
  (require-non-blank-string! db-identity :db-identity)
  (if (instance? SofaJraftLeaseAuthority authority)
    (let [{:keys [group-id running-v]} authority]
      (ensure-running! running-v)
      (lease/validate-lease-key! group-id db-identity)
      (submit-read-state-command! authority db-identity))
    (read-state authority db-identity)))

(defn new-in-memory-authority
  "Create an in-memory authority adapter for deterministic tests."
  [{:keys [group-id voters clock-skew-budget-ms scope-id]}]
  (lease/validate-membership-hash-key! group-id)
  (let [initial-voters (if (seq voters)
                         (validated-peer-ids! voters :ha-control-plane-voters)
                         [])
        scope-id (or scope-id *in-memory-group-scope*)]
    (->InMemoryLeaseAuthority group-id
                              (group-state scope-id group-id)
                              (volatile! false)
                              initial-voters
                              (some-> clock-skew-budget-ms
                                      (validate-clock-skew-budget!
                                       :clock-skew-budget-ms)
                                      long))))

(defn new-sofa-jraft-authority
  "Create the SOFAJRaft-backed distributed lease authority."
  [{:keys [group-id local-peer-id voters rpc-timeout-ms
           election-timeout-ms operation-timeout-ms
           raft-dir clock-skew-budget-ms]}]
  (lease/validate-membership-hash-key! group-id)
  (let [rpc-timeout-ms       (long (or rpc-timeout-ms
                                       default-rpc-timeout-ms))
        election-timeout-ms  (long (or election-timeout-ms
                                       default-election-timeout-ms))
        operation-timeout-ms (long (or operation-timeout-ms
                                       default-operation-timeout-ms))
        clock-skew-budget-ms
        (some-> clock-skew-budget-ms
                (validate-clock-skew-budget! :clock-skew-budget-ms)
                long)]
    (->SofaJraftLeaseAuthority group-id
                               local-peer-id
                               voters
                               rpc-timeout-ms
                               election-timeout-ms
                               operation-timeout-ms
                               raft-dir
                               clock-skew-budget-ms
                               (atom (blank-state))
                               (volatile! nil)
                               (volatile! nil)
                               (volatile! nil)
                               (volatile! false))))

(defn new-authority
  "Create an authority adapter by backend keyword."
  [{:keys [backend] :as opts}]
  (case backend
    :in-memory (new-in-memory-authority opts)
    :sofa-jraft (new-sofa-jraft-authority opts)
    (u/raise "Unsupported HA control-plane backend"
             {:error :ha/unsupported-backend
              :backend backend})))
