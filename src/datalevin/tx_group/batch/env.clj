;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.env
  "Private environment open and routing for the new write protocol.

  M0 wiring only. One canonical environment selects one collector protocol when
  it is first opened, and every handle into that environment shares that
  selection: there is no per-call switch and no fallback to compatibility after
  admission or body evaluation. Public KV, Datalog, remote/HA and shared-WAL
  entry points stay on `datalevin.tx-group.compat` until their own migration
  gates pass; nothing here routes them."
  (:require [datalevin.tx-group :as compat-group]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-state.protocol :as protocol])
  (:import [java.io File]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicBoolean AtomicInteger]))

(def ^:private environments
  "Canonical environment path -> its single runtime record."
  (ConcurrentHashMap.))

(deftype Environment [^String dir mode collector compat-collector lease
                      ^AtomicInteger handles closed?
                      limits])

(defn mode-name
  "The one protocol selected for an environment."
  ^String [^Environment environment]
  (.-mode environment))

(defn collector
  "The additive collector for a new-protocol environment. Compatibility
  environments have none."
  ^datalevin.tx_group.batch.Collector [^Environment environment]
  (let [c (.-collector environment)]
    (when (nil? c)
      (throw (ex-info "Environment is not on the new write protocol"
                      {:error :txlog/write-protocol-mismatch
                       :mode (.-mode environment)
                       :retryable? false})))
    c))

(defn compat-handle
  "A compatibility collector for a compatibility environment. New-protocol
  environments have none."
  [^Environment environment]
  (let [c (.-compat-collector environment)]
    (when (nil? c)
      (throw (ex-info "Environment is not on the compatibility write protocol"
                      {:error :txlog/write-protocol-mismatch
                       :mode (.-mode environment)
                       :retryable? false})))
    c))

(defn limits
  "Resolved, frozen limits for a new-protocol environment."
  [^Environment environment]
  (.-limits environment))

(defn handles
  "Live handle count for an environment."
  [^Environment environment]
  (.get ^AtomicInteger (.-handles environment)))

(defn- canonical [^File dir]
  (.getCanonicalPath dir))

(defn- build-collector
  "Construct the runtime's collector. `executor` owns ordered work, dispatch,
  join and local publication for this environment."
  [executor opts]
  (batch/create executor opts))

(defn- install!
  [dir mode record]
  (.put environments dir record)
  record)

(defn open-batch!
  "Open (or attach to) one new-protocol environment and return its runtime.

  `opts` supplies `:dir`, the required `:db-identity` of the environment, the
  `:executor` for its batches and optional byte-measure overrides. Selection
  happens once per canonical environment under its write-protocol lease: a
  second open of the same environment in compatibility mode is rejected rather
  than silently mixed, and different environments may select different
  protocols concurrently."
  [{:keys [dir db-identity executor wal-pending-max-requests wal-pending-max-bytes
           write-batch-size write-batch-max-bytes wal-rmw-max-bytes]
    :as opts}]
  (let [path (canonical (File. ^String dir))
        limits (charge/resolve-limits
                (select-keys opts [:wal-pending-max-requests :wal-pending-max-bytes
                                   :write-batch-size :write-batch-max-bytes
                                   :wal-rmw-max-bytes]))
        record (or (.get environments path)
                   (locking environments
                     (or (.get environments path)
                         (let [lease (protocol/acquire-write-protocol-lease!
                                      path :kv-independent-v1 db-identity)
                               record (->Environment path :kv-independent-v1
                                                    (build-collector executor
                                                                     {:limits limits})
                                                    nil lease
                                                    (AtomicInteger. 0)
                                                    (AtomicBoolean. true)
                                                    limits)]
                           (install! path :kv-independent-v1 record)))))]
    (when-not (= :kv-independent-v1 (mode-name record))
      (throw (ex-info "Environment write protocol does not match; use quiescent recovery"
                      {:error :txlog/write-protocol-mismatch
                       :dir path
                       :expected :kv-independent-v1
                       :actual (mode-name record)
                       :retryable? false})))
    (.incrementAndGet ^AtomicInteger (.-handles record))
    record))

(defn open-compat!
  "Open (or attach to) one compatibility environment and return its runtime.

  Compatibility keeps the unchanged `datalevin.tx-group` collector under its
  shared `legacy-writer-v1` protocol lease. Selecting it for an environment that
  already runs the new protocol is rejected before any mutation."
  [{:keys [dir db-identity limit] :as opts}]
  (let [path (canonical (File. ^String dir))
        record (or (.get environments path)
                   (locking environments
                     (or (.get environments path)
                         (let [lease (protocol/acquire-write-protocol-lease!
                                      path :legacy-writer-v1 db-identity)
                               record (->Environment path :legacy-writer-v1
                                                    nil
                                                    (compat-group/create
                                                     (long (or limit 256)))
                                                    lease (AtomicInteger. 0)
                                                    (AtomicBoolean. true) nil)]
                           (install! path :legacy-writer-v1 record)))))]
    (when-not (= :legacy-writer-v1 (mode-name record))
      (throw (ex-info "Environment write protocol does not match; use quiescent recovery"
                      {:error :txlog/write-protocol-mismatch
                       :dir path
                       :expected :legacy-writer-v1
                       :actual (mode-name record)
                       :retryable? false})))
    (.incrementAndGet ^AtomicInteger (.-handles record))
    record))

(defn- teardown!
  [^Environment environment]
  (when (.compareAndSet ^AtomicBoolean (.-closed? environment) false true)
    (let [^AtomicInteger handles (.-handles environment)]
      (when (zero? (.get handles))
        (try
          (when-let [c (.-collector environment)] (batch/close! c))
          (finally
            (.remove environments (.-dir environment))
            (protocol/release! (.-lease environment)))))))
  nil)

(defn close!
  "Release one handle. The last handle closes the runtime and, for a
  new-protocol environment, stops admission."
  [^Environment environment]
  (when (pos? (.decrementAndGet ^AtomicInteger (.-handles environment)))
    nil)
  (teardown! environment))

(defn active-environments
  "Registered canonical paths. Diagnostics only."
  []
  (vec (.keySet environments)))

(defn reset-registry!
  "Drop registry entries without releasing leases. Fault-harness cleanup only."
  []
  (.clear environments)
  nil)
