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
   [com.alipay.sofa.jraft.option NodeOptions]
   [com.alipay.sofa.jraft.rpc RpcContext RpcProcessor RpcClient
    RpcRequests$ErrorResponse ProtobufMsgFactory]
   [com.alipay.sofa.jraft.storage.snapshot SnapshotReader SnapshotWriter]
   [com.alipay.sofa.jraft.util RpcFactoryHelper]
   [datalevin.ha LMDBJRaftServiceFactory]
   [java.io File]
   [java.nio ByteBuffer]
   [java.nio.file Files Paths StandardCopyOption]
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

(defonce ^:private in-memory-groups
  (atom {}))

(defn- group-state
  [group-id]
  (or (get @in-memory-groups group-id)
      (let [state (atom (blank-state))]
        (get (swap! in-memory-groups
                    (fn [m]
                      (if (contains? m group-id)
                        m
                        (assoc m group-id state))))
             group-id))))

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

(defrecord InMemoryLeaseAuthority [group-id state running-v initial-voters]
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
    (lease/lease-key group-id db-identity)
    (let [{:keys [lease version]} (lease-entry @state db-identity)]
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
    (lease/lease-key group-id db-identity)
    (let [result-v (volatile! nil)]
      (swap! state
             (fn [s]
               (let [{:keys [lease version]} (lease-entry s db-identity)
                     observed-version (long (or observed-version 0))
                     current-version (long version)]
                 (cond
                   (not= observed-version current-version)
                   (do (vreset! result-v
                                {:ok? false
                                 :reason :cas-mismatch
                                 :lease lease
                                 :version current-version})
                       s)

                   (and (some? observed-lease) (not= observed-lease lease))
                   (do (vreset! result-v
                                {:ok? false
                                 :reason :observed-lease-mismatch
                                 :lease lease
                                 :version current-version})
                       s)

                   (and lease (not= db-identity (:db-identity lease)))
                   (do (vreset! result-v
                                {:ok? false
                                 :reason :db-identity-mismatch
                                 :lease lease
                                 :version current-version})
                       s)

                   (and lease (not (lease/lease-expired? lease now-ms)))
                   (do (vreset! result-v
                                {:ok? false
                                 :reason :lease-not-expired
                                 :lease lease
                                 :version current-version})
                       s)

                   :else
                   (let [observed (or observed-lease lease)
                         new-term (lease/next-term observed)
                         new-lease (lease/new-lease-record
                                     {:db-identity db-identity
                                      :leader-node-id leader-node-id
                                      :leader-endpoint leader-endpoint
                                      :term new-term
                                      :lease-renew-ms lease-renew-ms
                                      :lease-timeout-ms lease-timeout-ms
                                      :now-ms now-ms
                                      :leader-last-applied-lsn
                                      leader-last-applied-lsn})
                         new-version (inc current-version)]
                     (vreset! result-v
                              {:ok? true
                               :lease new-lease
                               :version new-version
                               :term new-term})
                     (assoc-in s [:leases db-identity]
                               {:lease new-lease
                                :version new-version}))))))
      @result-v))

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
    (lease/lease-key group-id db-identity)
    (let [result-v (volatile! nil)]
      (swap! state
             (fn [s]
               (let [{:keys [lease version]} (lease-entry s db-identity)
                     current-version (long version)]
                 (cond
                   (nil? lease)
                   (do (vreset! result-v
                                {:ok? false
                                 :reason :missing-lease
                                 :version current-version})
                       s)

                   (not= db-identity (:db-identity lease))
                   (do (vreset! result-v
                                {:ok? false
                                 :reason :db-identity-mismatch
                                 :lease lease
                                 :version current-version})
                       s)

                   (lease/lease-expired? lease now-ms)
                   (do (vreset! result-v
                                {:ok? false
                                 :reason :lease-expired
                                 :lease lease
                                 :version current-version})
                       s)

                   (not= leader-node-id (:leader-node-id lease))
                   (do (vreset! result-v
                                {:ok? false
                                 :reason :owner-mismatch
                                 :lease lease
                                 :version current-version})
                       s)

                   (not= term (:term lease))
                   (do (vreset! result-v
                                {:ok? false
                                 :reason :term-mismatch
                                 :lease lease
                                 :version current-version})
                       s)

                   :else
                   (let [new-lease (lease/new-lease-record
                                     {:db-identity db-identity
                                      :leader-node-id leader-node-id
                                      :leader-endpoint leader-endpoint
                                      :term term
                                      :lease-renew-ms lease-renew-ms
                                      :lease-timeout-ms lease-timeout-ms
                                      :now-ms now-ms
                                      :leader-last-applied-lsn
                                      leader-last-applied-lsn})
                         new-version (inc current-version)]
                     (vreset! result-v
                              {:ok? true
                               :lease new-lease
                               :version new-version
                               :term term})
                     (assoc-in s [:leases db-identity]
                               {:lease new-lease
                                :version new-version}))))))
      @result-v))

  (read-membership-hash [_]
    (ensure-running! running-v)
    (:membership-hash @state))

  (init-membership-hash! [_ membership-hash]
    (ensure-running! running-v)
    (require-non-blank-string! membership-hash :membership-hash)
    (lease/membership-hash-key group-id)
    (let [result-v (volatile! nil)]
      (swap! state
             (fn [s]
               (let [existing (:membership-hash s)]
                 (cond
                   (nil? existing)
                   (do (vreset! result-v
                                {:ok? true
                                 :initialized? true
                                 :membership-hash membership-hash})
                       (assoc s :membership-hash membership-hash))

                   (= existing membership-hash)
                   (do (vreset! result-v
                                {:ok? true
                                 :initialized? false
                                 :membership-hash existing})
                       s)

                   :else
                   (do (vreset! result-v
                                {:ok? false
                                 :reason :membership-hash-mismatch
                                 :membership-hash existing
                                 :expected membership-hash})
                       s)))))
      @result-v))

  (read-voters [_]
    (ensure-running! running-v)
    (vec (:voters @state)))

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
                 observed-version observed-lease] :as req}]
  (validate-acquire-request! req)
  (let [{:keys [lease version]} (lease-entry state db-identity)
        observed-version (long (or observed-version 0))
        current-version (long version)]
    (cond
      (not= observed-version current-version)
      {:state state
       :result {:ok? false
                :reason :cas-mismatch
                :lease lease
                :version current-version}}

      (and (some? observed-lease) (not= observed-lease lease))
      {:state state
       :result {:ok? false
                :reason :observed-lease-mismatch
                :lease lease
                :version current-version}}

      (and lease (not= db-identity (:db-identity lease)))
      {:state state
       :result {:ok? false
                :reason :db-identity-mismatch
                :lease lease
                :version current-version}}

      (and lease (not (lease/lease-expired? lease now-ms)))
      {:state state
       :result {:ok? false
                :reason :lease-not-expired
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
                          :now-ms now-ms
                          :leader-last-applied-lsn leader-last-applied-lsn})
            new-version (inc current-version)]
        {:state (assoc-in state [:leases db-identity]
                          {:lease new-lease
                           :version new-version})
         :result {:ok? true
                  :lease new-lease
                  :version new-version
                  :term new-term}}))))

(defn- apply-renew-transition
  [state {:keys [db-identity leader-node-id leader-endpoint
                 term lease-renew-ms lease-timeout-ms
                 leader-last-applied-lsn now-ms] :as req}]
  (validate-renew-request! req)
  (let [{:keys [lease version]} (lease-entry state db-identity)
        current-version (long version)]
    (cond
      (nil? lease)
      {:state state
       :result {:ok? false
                :reason :missing-lease
                :version current-version}}

      (not= db-identity (:db-identity lease))
      {:state state
       :result {:ok? false
                :reason :db-identity-mismatch
                :lease lease
                :version current-version}}

      (lease/lease-expired? lease now-ms)
      {:state state
       :result {:ok? false
                :reason :lease-expired
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
      (let [new-lease   (lease/new-lease-record
                         {:db-identity db-identity
                          :leader-node-id leader-node-id
                          :leader-endpoint leader-endpoint
                          :term term
                          :lease-renew-ms lease-renew-ms
                          :lease-timeout-ms lease-timeout-ms
                          :now-ms now-ms
                          :leader-last-applied-lsn leader-last-applied-lsn})
            new-version (inc current-version)]
        {:state (assoc-in state [:leases db-identity]
                          {:lease new-lease
                           :version new-version})
         :result {:ok? true
                  :lease new-lease
                  :version new-version
                  :term term}}))))

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
  (require-non-blank-string! db-identity :db-identity)
  (let [{:keys [lease version]} (lease-entry state db-identity)]
    {:state state
     :result {:lease lease
              :version version
              :membership-hash (:membership-hash state)
              :voters (:voters state)}}))

(defn- apply-state-command
  [state {:keys [op] :as cmd}]
  (case op
    :try-acquire-lease   (apply-try-acquire-transition state (:req cmd))
    :renew-lease         (apply-renew-transition state (:req cmd))
    :init-membership-hash (apply-init-membership-hash-transition
                           state (:membership-hash cmd))
    :read-state          (apply-read-state-transition state (:db-identity cmd))
    (u/raise "Unsupported HA control command"
             {:error :ha/control-invalid-command
              :command cmd})))

(defn- apply-state-command!
  [state-atom cmd]
  (let [result-v (volatile! nil)]
    (swap! state-atom
           (fn [s]
             (let [{:keys [state result]} (apply-state-command s cmd)]
               (vreset! result-v result)
               state)))
    @result-v))

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

(defn- move-replace-existing!
  [^String from ^String to]
  (let [from-path (Paths/get from (make-array String 0))
        to-path   (Paths/get to (make-array String 0))]
    (try
      (Files/move from-path
                  to-path
                  ^"[Ljava.nio.file.CopyOption;"
                  (into-array java.nio.file.CopyOption
                              [StandardCopyOption/REPLACE_EXISTING
                               StandardCopyOption/ATOMIC_MOVE]))
      (catch Exception _
        (Files/move from-path
                    to-path
                    ^"[Ljava.nio.file.CopyOption;"
                    (into-array java.nio.file.CopyOption
                                [StandardCopyOption/REPLACE_EXISTING]))))))

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

(defn- command->byte-buffer
  [cmd]
  (ByteBuffer/wrap ^bytes (nippy/freeze cmd)))

(defn- apply-local-command-once!
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

(declare authority-diagnostics)

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
                                           (run [^Status status _index _request-ctx]
                                             (deliver status-p status))))
                             true
                             (catch Exception e
                               (log/warn e "HA control readIndex invocation failed")
                               false))]
            (if-not invoked?
              (do (Thread/sleep 20)
                  (recur (inc attempt)))
              (let [status (deref status-p timeout-ms ::timeout)]
                (cond
                  (= ::timeout status)
                  (if (and (single-voter-authority? authority)
                           (.isLeader node))
                    true
                    (do (Thread/sleep 20)
                        (recur (inc attempt))))

                  (.isOk ^Status status)
                  true

                  (retryable-read-status? ^Status status)
                  (do (Thread/sleep 20)
                      (recur (inc attempt)))

                  :else
                  (u/raise "HA control readIndex failed"
                           {:error :ha/control-read-failed
                            :status (status-data ^Status status)
                            :authority (authority-diagnostics authority)}))))))))))

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
                              (let [cmd (:command payload)
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
  [{:keys [rpc-timeout-ms operation-timeout-ms] :as authority} cmd]
  (let [deadline (+ (System/currentTimeMillis) (long operation-timeout-ms))]
    (loop [attempt 0]
      (let [remaining (- deadline (System/currentTimeMillis))]
        (when (<= remaining 0)
          (u/raise "HA control command timed out"
                   {:error :ha/control-timeout
                    :attempt attempt
                    :command (:op cmd)}))
        (let [{:keys [^Node node ^RpcClient rpc-client]}
              (running-runtime! authority)]
          (if (.isLeader node)
            (let [local-res (apply-local-command-once!
                             node cmd (max 1 remaining))]
              (cond
                (:ok? local-res)
                (:result local-res)

                (#{:not-leader :timeout} (:error local-res))
                (do (Thread/sleep 20)
                    (recur (inc attempt)))

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
                    invoke-timeout (long (max 1 (min remaining
                                                     (long rpc-timeout-ms))))
                    response (try
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
                                 (log/warn e "HA control forward failed"
                                           {:attempt attempt
                                            :leader (peer-id-string leader)})
                                 ::invoke-failed))]
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
                      (:result payload)

                      (contains? #{:not-leader :apply-timeout :node-unavailable}
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

(defn- new-jraft-fsm
  [state-atom]
  (proxy [StateMachineAdapter] []
    (onApply [^Iterator iter]
      (try
        (loop []
          (when (.hasNext iter)
            (let [^ByteBuffer data (.getData iter)
                  done           (.done iter)
                  cmd            (nippy/thaw (bytebuffer->bytes data))
                  result         (apply-state-command! state-atom cmd)]
              (when (instance? CommandClosure done)
                (vreset! (:result-v done) result)
                (.run ^CommandClosure done (Status/OK)))
              (.next iter)
              (recur))))
        (catch Exception e
          (log/error e "HA control JRaft FSM apply failed")
          (.setErrorAndRollback iter
                                1
                                (Status.
                                 (int (.getNumber RaftError/ESTATEMACHINE))
                                 (str "HA FSM apply failed: "
                                      (.getMessage e)))))))

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
    (lease/lease-key group-id db-identity)
    (await-linearizable-read! this)
    (let [{:keys [lease version]} (lease-entry @fsm-state db-identity)]
      {:lease lease
       :version version}))

  (try-acquire-lease [this req]
    (ensure-running! running-v)
    (lease/lease-key group-id (:db-identity req))
    (submit-command! this {:op :try-acquire-lease
                           :req req}))

  (renew-lease [this req]
    (ensure-running! running-v)
    (lease/lease-key group-id (:db-identity req))
    (submit-command! this {:op :renew-lease
                           :req req}))

  (read-membership-hash [this]
    (ensure-running! running-v)
    (lease/membership-hash-key group-id)
    (await-linearizable-read! this)
    (:membership-hash @fsm-state))

  (init-membership-hash! [this membership-hash]
    (ensure-running! running-v)
    (lease/membership-hash-key group-id)
    (submit-command! this {:op :init-membership-hash
                           :membership-hash membership-hash}))

  (read-voters [this]
    (ensure-running! running-v)
    (await-linearizable-read! this)
    (node-peer-ids (running-node! this)))

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
              (let [local-res (change-peers-once!
                               node peer-ids (max 1 remaining))]
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
                      response (try
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
                                   (log/warn e "HA control forward failed"
                                             {:attempt attempt
                                              :leader (peer-id-string leader)})
                                   ::invoke-failed))]
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
      (let [{:keys [group-id state running-v initial-voters]} authority
            snapshot @state]
        {:backend :in-memory
         :group-id group-id
         :running? (running? running-v)
         :initial-voters initial-voters
         :voters (:voters snapshot)
         :membership-hash (:membership-hash snapshot)
         :lease-count (count (:leases snapshot))})

      (instance? SofaJraftLeaseAuthority authority)
      (let [{:keys [group-id local-peer-id voters
                    rpc-timeout-ms election-timeout-ms
                    operation-timeout-ms fsm-state node-v
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
         :fsm-voters (:voters snapshot)
         :fsm-membership-hash (:membership-hash snapshot)
         :fsm-lease-count (count (:leases snapshot))
         :node-available? (some? node)
         :node-leader? (when node (.isLeader node))
         :node-state (when node (safe-node-value #(.getNodeState node)))
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

  For the SOFAJRaft backend this uses the replicated command path instead of
  readIndex, because the full DB-server HA startup path can stall in readIndex
  even when the raft group is otherwise healthy."
  [authority db-identity]
  (require-non-blank-string! db-identity :db-identity)
  (cond
    (instance? InMemoryLeaseAuthority authority)
    (let [{:keys [group-id state running-v]} authority]
      (ensure-running! running-v)
      (lease/lease-key group-id db-identity)
      (let [{:keys [lease version]} (lease-entry @state db-identity)]
        {:lease lease
         :version version
         :membership-hash (:membership-hash @state)
         :voters (:voters @state)}))

    (instance? SofaJraftLeaseAuthority authority)
    (let [{:keys [group-id running-v]} authority]
      (ensure-running! running-v)
      (lease/lease-key group-id db-identity)
      (submit-command! authority {:op :read-state
                                  :db-identity db-identity}))

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

(defn new-in-memory-authority
  "Create an in-memory authority adapter for deterministic tests."
  [{:keys [group-id voters]}]
  (lease/membership-hash-key group-id)
  (let [initial-voters (if (seq voters)
                         (validated-peer-ids! voters :ha-control-plane-voters)
                         [])]
    (->InMemoryLeaseAuthority group-id
                              (group-state group-id)
                              (volatile! false)
                              initial-voters)))

(defn new-sofa-jraft-authority
  "Create the SOFAJRaft-backed distributed lease authority."
  [{:keys [group-id local-peer-id voters rpc-timeout-ms
           election-timeout-ms operation-timeout-ms
           raft-dir]}]
  (lease/membership-hash-key group-id)
  (let [rpc-timeout-ms       (long (or rpc-timeout-ms
                                       default-rpc-timeout-ms))
        election-timeout-ms  (long (or election-timeout-ms
                                       default-election-timeout-ms))
        operation-timeout-ms (long (or operation-timeout-ms
                                       default-operation-timeout-ms))]
    (->SofaJraftLeaseAuthority group-id
                               local-peer-id
                               voters
                               rpc-timeout-ms
                               election-timeout-ms
                               operation-timeout-ms
                               raft-dir
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
