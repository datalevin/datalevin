;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.env
  "Private environment open and routing for the new write protocol.

  M0 wiring only. One canonical environment selects one collector protocol when
  it is first opened, and every handle into that environment shares that
  selection: there is no per-call switch and no fallback to compatibility after
  admission or body evaluation. Public KV, Datalog, remote/HA and shared-WAL
  entry points stay on `datalevin.tx-group.compat` until their own migration
  gates pass; nothing here routes them.

  Registry membership, handle counts and the closed flag are all mutated under
  the `environments` monitor, so an open can never attach to a runtime that a
  concurrent close has already retired. Close marks the runtime closed under
  that monitor, then waits for collector quiescence outside it and only removes
  the entry and releases the lease once the executing batch has drained. If
  quiescence cannot be confirmed, the closed record and its lease are retained
  so no replacement can open; recovery requires confirmed process exit."
  (:require [datalevin.tx-group :as compat-group]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-state.protocol :as protocol])
  (:import [java.io File]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicBoolean AtomicInteger]))

(def ^:private default-close-timeout-ms 30000)

(def ^:private environments
  "Canonical environment path -> its single runtime record."
  (ConcurrentHashMap.))

(deftype Environment [^String dir mode db-identity collector compat-collector
                      lease ^AtomicInteger handles closed? ^long close-timeout-ms
                      limits executor-close! resources])

(defn resources
  "Private opener resources shared by every handle; never a public KV handle."
  [^Environment environment]
  (.-resources environment))

(defn mode-name
  "The one protocol selected for an environment."
  [^Environment environment]
  (.-mode environment))

(defn db-identity
  "Persisted database identity this runtime was opened with."
  [^Environment environment]
  (.-db-identity environment))

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

(defn- check-openable!
  "Validate one attach against the registered runtime. The caller holds the
  `environments` monitor."
  [^Environment environment path expected-mode identity]
  (when (.get ^AtomicBoolean (.-closed? environment))
    (throw (ex-info "Environment runtime is closed; a replacement requires quiescence or process exit"
                    {:error :txlog/runtime-closed
                     :dir path
                     :retryable? false})))
  (when-not (= expected-mode (mode-name environment))
    (throw (ex-info "Environment write protocol does not match; use quiescent recovery"
                    {:error :txlog/write-protocol-mismatch
                     :dir path
                     :expected expected-mode
                     :actual (mode-name environment)
                     :retryable? false})))
  (when-not (= (.-db-identity environment) identity)
    (throw (ex-info "Environment database identity does not match"
                    {:error :txlog/database-identity-mismatch
                     :dir path
                     :expected (.-db-identity environment)
                     :actual identity
                     :retryable? false}))))

(defn- build-collector
  "Construct the runtime's collector. `executor` owns ordered work, dispatch,
  join and local publication for this environment."
  [executor opts]
  (batch/create executor opts))

(defn- install!
  [dir record]
  (.put environments dir record)
  record)

(defn- close-timeout-ms
  ^long [opts]
  (long (or (:write-close-timeout-ms opts)
            (:wal-close-timeout-ms opts)
            default-close-timeout-ms)))

(defn open-batch!
  "Open (or attach to) one new-protocol environment and return its runtime.

  `opts` supplies `:dir`, the required `:db-identity` of the environment, the
  `:executor` for its batches, an optional `:executor-close!` that drains the
  executor's worker after collector quiescence, and optional byte-measure
  overrides. Selection happens once per canonical environment under its
  write-protocol lease: a second open of the same environment in compatibility
  mode, or with a different database identity, is rejected rather than silently
  mixed. Alternatively `:open-runtime!` builds {:executor :close! :resources}
  under the acquired lease, once per environment. It must clean up on failure;
  :close! drains the worker and closes native/WAL resources after quiescence.
  An optional :bind! receives the collector before the runtime is published.
  Different environments may select different protocols concurrently."
  [{:keys [dir db-identity executor executor-close! open-runtime!] :as opts}]
  (let [path (canonical (File. ^String dir))
        limits (charge/resolve-limits
                (select-keys opts [:wal-pending-max-requests :wal-pending-max-bytes
                                   :write-batch-size :write-batch-max-bytes
                                   :wal-rmw-max-bytes]))
        preparation-timeout-ms
        (let [v (get opts :wal-preparation-timeout-ms
                     batch/default-preparation-timeout-ms)]
          (when-not (pos-int? v)
            (throw (ex-info "Preparation timeout must be a positive integer"
                            {:error :txlog/invalid-preparation-timeout :value v})))
          (long v))
        timeout-ms (close-timeout-ms opts)]
    (locking environments
      (if-let [record (.get environments path)]
        (do (check-openable! record path :kv-independent-v1 db-identity)
            (when (and (contains? opts :wal?)
                       (contains? (resources record) :wal?)
                       (not= (:wal? opts) (:wal? (resources record))))
              (throw (ex-info "Cannot mix WAL and native-only handles"
                              {:error :txlog/write-protocol-mismatch :dir path})))
            (.incrementAndGet ^AtomicInteger (.-handles record))
            record)
        (let [lease (protocol/acquire-write-protocol-lease!
                     path :kv-independent-v1 db-identity)]
          (try
            (let [runtime (if open-runtime! (open-runtime!)
                              {:executor executor :close! executor-close!})]
              (try
                (let [c (build-collector (:executor runtime)
                                         {:limits limits
                                          :preparation-timeout-ms preparation-timeout-ms
                                          :check-prepared! (:check-prepared! runtime)
                                          :on-failure! (:on-failure! runtime)})
                      record (->Environment path :kv-independent-v1 db-identity
                                            c nil lease (AtomicInteger. 1)
                                            (AtomicBoolean. false) timeout-ms limits
                                            (:close! runtime) (:resources runtime))]
                  (when-let [bind! (:bind! runtime)] (bind! c))
                  (install! path record))
                (catch Throwable t
                  (when-let [close! (:close! runtime)] (close!))
                  (throw t))))
            (catch Throwable t
              (protocol/release! lease)
              (throw t))))))))

(defn open-compat!
  "Open (or attach to) one compatibility environment and return its runtime.

  Compatibility keeps the unchanged `datalevin.tx-group` collector under its
  shared `legacy-writer-v1` protocol lease. Selecting it for an environment that
  already runs the new protocol, or with a different database identity, is
  rejected before any mutation."
  [{:keys [dir db-identity limit] :as opts}]
  (let [path (canonical (File. ^String dir))
        timeout-ms (close-timeout-ms opts)]
    (locking environments
      (if-let [record (.get environments path)]
        (do (check-openable! record path :legacy-writer-v1 db-identity)
            (.incrementAndGet ^AtomicInteger (.-handles record))
            record)
        (let [lease (protocol/acquire-write-protocol-lease!
                     path :legacy-writer-v1 db-identity)
              record (->Environment path :legacy-writer-v1 db-identity
                                    nil (compat-group/create (long (or limit 256)))
                                    lease (AtomicInteger. 1)
                                    (AtomicBoolean. false) timeout-ms nil
                                    nil nil)]
          (install! path record))))))

(defn- release-runtime!
  "Remove the retired record and release its lease under the registry monitor,
  so no concurrent open can interleave with the release. The runtime must
  already be fenced and drained."
  [^Environment environment]
  (locking environments
    (.remove environments (.-dir environment))
    (protocol/release! (.-lease environment))))

(defn close!
  "Release one handle.

  The last handle marks the runtime closed under the registry monitor, fences
  the collector so no new work is admitted, then waits up to its close timeout
  for the executing batch to drain. Only once quiescence is confirmed does it
  drain the environment executor's worker, so WAL maintenance cannot outlive the
  collector. If quiescence times out, the executor is left running — a live batch
  may still be awaiting its WAL task — and the closed record and lease are
  retained; later opens are rejected, so a replacement requires confirmed process
  exit."
  [^Environment environment]
  (let [last? (locking environments
                (when (zero? (.decrementAndGet ^AtomicInteger (.-handles environment)))
                  ;; Mark the runtime closed in the same critical section as the
                  ;; last-handle decision. An open racing this close either
                  ;; attaches before the decrement (and keeps a handle, so this
                  ;; is not the last one) or acquires the registry lock afterward
                  ;; and observes the closed flag. Setting it outside the lock
                  ;; let an open attach between the decrement and retirement.
                  (.set ^AtomicBoolean (.-closed? environment) true)
                  true))]
    (when last?
      (let [c (.-collector environment)
            quiescent? (if c
                         (do (batch/close! c)
                             (batch/await-quiescence! c
                                                      (.-close-timeout-ms environment)))
                         true)]
        ;; Stop the executor's worker only after the collector is confirmed
        ;; drained. If quiescence timed out, a live batch may be awaiting a WAL
        ;; task on that worker; stopping it would discard accepted work and could
        ;; strand the batch permanently. Retain the record and lease instead.
        (when quiescent?
          (let [drained? (if-let [close-executor (.-executor-close! environment)]
                           (boolean (close-executor))
                           true)]
            (when drained?
              (release-runtime! environment))))))
    nil))

(defn active-environments
  "Registered canonical paths. Diagnostics only."
  []
  (vec (.keySet environments)))

(defn reset-registry!
  "Drop registry entries without releasing leases. Fault-harness cleanup only."
  []
  (.clear environments)
  nil)
