;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.charge
  "Deterministic allocation charges and resolved limits for the new write
  protocol's byte measure.

  Charges are conservative estimates of engine-owned storage, not measured
  object sizes. They exist so admission, batch selection and the shared
  workspace can be bounded with primitive arithmetic that performs no graph
  walk and allocates no ledger. Every addition, product and rounding step is
  checked, so an unrepresentable declaration fails instead of silently
  wrapping.")

(defn- overflow!
  [what ^long n]
  (throw (ex-info "Allocation charge overflowed"
                  {:error :txlog/pending-budget-exceeded
                   :outcome :not-committed :retryable? false
                   :charge what :value n})))

(defn align16
  "Round a non-negative charge up to a multiple of 16, checked."
  ^long [^long n]
  (when (neg? n) (overflow! :negative n))
  (let [padded (Math/addExact n (bit-and (- n) 15))]
    (when (neg? padded) (overflow! :align16 padded))
    padded))

(defn array-bytes
  "`A(w, n)`: an array of element width `w` and allocated capacity `n`.
  An empty array still costs its header."
  ^long [^long width ^long capacity]
  (when (neg? capacity) (overflow! :negative-capacity capacity))
  (align16 (Math/addExact 32 (Math/multiplyExact width capacity))))

(defn string-bytes
  "`64 + A(2, character-count)` for an owned string."
  ^long [^long characters]
  (when (neg? characters) (overflow! :negative-characters characters))
  (Math/addExact 64 (array-bytes 2 characters)))

(defn vector-bytes
  "`64` for the wrapper plus `A(8, capacity)` for backing references."
  ^long [^long capacity]
  (Math/addExact 64 (array-bytes 8 capacity)))

(defn carriers-bytes
  "`V(n) = 64 + A(8, n)`: one membership/result/gather reference carrier."
  ^long [^long capacity]
  (Math/addExact 64 (array-bytes 8 capacity)))

;; Fixed charges. Each covers the whole owned representation it names,
;; including its synchronization wrappers; variable storage is additional.
(def ^:const request-control-bundle
  "Descriptor, queue link, admission/ownership fields, context control, result
  slot and notification."
  1024)

(def ^:const encoded-row-descriptor 128)
(def ^:const ordered-map-container 128)
(def ^:const ordered-map-entry 128)
(def ^:const per-key-staging-state 128)
(def ^:const vector-wrapper 64)
(def ^:const buffer-wrapper 256)
(def ^:const scalar-box 64)

(def ^:private ^:const min-request-allowance
  "Every admitted request must at least cover its own control bundle."
  request-control-bundle)

(def ^:private ^:const wal-control-bytes
  "96 KiB for WAL controls/scratch, including the at-most-64-KiB compact record
  buffer and record headers."
  (* 96 1024))

(def ^:private ^:const branch-control-bytes
  "32 KiB for collector/native branch controls."
  (* 32 1024))

(def ^:private ^:const shared-workspace-bytes
  (Math/addExact wal-control-bytes branch-control-bytes))

(def ^:private ^:const waiter-control-bytes 256)

(defn shared-reserved
  "`F = 128 KiB + 256*Q + 3*V(N + 2) + 256*(N + 2)`, checked.

  Reserved once when opening a new-mode runtime and never charged again against
  an individual request or the batch byte cap."
  ^long [^long batch-limit ^long waiter-limit]
  (let [slots (Math/addExact (long batch-limit) 2)]
    (Math/addExact
     shared-workspace-bytes
     (Math/addExact
      (Math/multiplyExact waiter-control-bytes (long waiter-limit))
      (Math/addExact
       (Math/multiplyExact 3 (carriers-bytes slots))
       (Math/multiplyExact waiter-control-bytes slots))))))

(defn blind-allowance
  "Declared whole-request allowance for a blind write, charged once before the
  caller allocates anything.

  The declaration must cover every planned owned allocation for the request:
  its control bundle, encoded row descriptors, owned key/value arrays, encode
  scratch with old/new growth overlap, the assembled record buffers, result
  storage and any deferred staging the descriptor might later materialize.
  Because it is prepaid, creating objects inside this layout performs no
  further `C_i` comparison; only unplanned growth re-enters the checked path."
  ^long [{:keys [declared-bytes row-capacity scratch-bytes result-capacity
                 duplicate-values]
          :or {row-capacity 1 scratch-bytes 0 result-capacity 1
               duplicate-values 0}}]
  (when (neg? (long declared-bytes)) (overflow! :negative-declared-bytes declared-bytes))
  (when (neg? (long row-capacity)) (overflow! :negative-row-capacity row-capacity))
  (when (neg? (long scratch-bytes)) (overflow! :negative-scratch-bytes scratch-bytes))
  (when (neg? (long result-capacity)) (overflow! :negative-result-capacity result-capacity))
  (when (neg? (long duplicate-values)) (overflow! :negative-duplicate-values duplicate-values))
  (let [declared-bytes (long declared-bytes)
        rows (Math/addExact 1 (long row-capacity))
        ;; One owned key array and one owned value array, plus one assembled
        ;; record buffer. Each wrapper is charged; growth charges old and new
        ;; capacity while the scratch is still owned.
        assembled (Math/addExact (Math/addExact declared-bytes (long scratch-bytes))
                                 (long scratch-bytes))]
    (Math/addExact
     request-control-bundle
     (Math/addExact
      (Math/multiplyExact encoded-row-descriptor rows)
      (Math/addExact
       ;; Each row can own separate key/value arrays. Summing payload bytes
       ;; alone misses their headers and alignment, especially for empty rows.
       (Math/addExact
        (Math/addExact declared-bytes
                       (Math/multiplyExact 94 (long row-capacity)))
        (array-bytes 1 assembled))
       (Math/addExact
        (Math/addExact
         (Math/multiplyExact buffer-wrapper
                            (Math/addExact 3 (Math/multiplyExact 2 (long row-capacity))))
         (Math/multiplyExact per-key-staging-state rows))
        (Math/addExact
         (Math/addExact ordered-map-container (Math/multiplyExact ordered-map-entry rows))
         (Math/addExact
          (vector-bytes (Math/addExact 1 (long result-capacity)))
          (Math/multiplyExact ordered-map-entry (long duplicate-values))))))))))

(defn rmw-allowance
  "Configured maximum allowance for a body-based request whose output size is
  not known in advance. Body-driven storage is charged incrementally against
  this allowance, so it covers the control bundle as well."
  ^long [^long configured-bytes]
  (let [n (long configured-bytes)]
    (when-not (>= n min-request-allowance)
      (throw (ex-info "RMW allowance cannot hold one minimum request allowance"
                      {:error :txlog/write-protocol-limits
                       :wal-rmw-max-bytes n})))
    n))

(defrecord Limits
  [max-requests byte-budget shared-reserved request-budget batch-limit
   batch-max-bytes waiter-limit])

(def default-limits
  "Initial new-mode limits. `N` reuses the existing batch size; the batch byte
  cap is separate and independent of the remaining global capacity."
  (map->Limits
   {:max-requests 4096
    :byte-budget 67108864
    :batch-limit 256
    :batch-max-bytes 8388608
    :waiter-limit 4096
    :rmw-allowance-bytes 1048576}))

(defn- positive-int! [what ^long n]
  (when-not (pos? n)
    (throw (ex-info "New write protocol limits must be positive integers"
                    {:error :txlog/write-protocol-limits :limit what :value n})))
  n)

(defn resolve-limits
  "Resolve, validate and freeze the byte measure for one new-mode runtime.

  Rejects incompatible settings instead of silently shrinking defaults or
  letting the batch cap fall back to remaining global capacity. Explicit values
  require a quiescent reopen, exactly like the RMW allowance."
  ^Limits [{:keys [wal-pending-max-requests wal-pending-max-bytes
                   write-batch-size write-batch-max-bytes wal-rmw-max-bytes]
            :or {wal-pending-max-requests (:max-requests default-limits)
                 wal-pending-max-bytes (:byte-budget default-limits)
                 write-batch-size (:batch-limit default-limits)
                 write-batch-max-bytes (:batch-max-bytes default-limits)
                 wal-rmw-max-bytes (:rmw-allowance-bytes default-limits)}}]
  (let [max-requests (positive-int! :wal-pending-max-requests
                                   (long wal-pending-max-requests))
        byte-budget (positive-int! :wal-pending-max-bytes (long wal-pending-max-bytes))
        batch-limit (positive-int! :write-batch-size (long write-batch-size))
        batch-max-bytes (positive-int! :write-batch-max-bytes (long write-batch-max-bytes))
        waiter-limit max-requests
        rmw-max (positive-int! :wal-rmw-max-bytes (long wal-rmw-max-bytes))]
    (when (> batch-limit max-requests)
      (throw (ex-info "Batch request count cannot exceed the admitted request limit"
                      {:error :txlog/write-protocol-limits
                       :write-batch-size batch-limit
                       :wal-pending-max-requests max-requests})))
    (let [reserved (shared-reserved batch-limit waiter-limit)]
      (when-not (< reserved byte-budget)
        (throw (ex-info "Shared workspace does not fit the environment byte budget"
                        {:error :txlog/write-protocol-limits
                         :shared-reserved reserved :byte-budget byte-budget})))
      (when-not (<= min-request-allowance batch-max-bytes)
        (throw (ex-info "Batch byte cap cannot hold one minimum request allowance"
                        {:error :txlog/write-protocol-limits
                         :write-batch-max-bytes batch-max-bytes})))
      (when (> batch-max-bytes (- byte-budget reserved))
        (throw (ex-info "Batch byte cap exceeds the remaining request-admission budget"
                        {:error :txlog/write-protocol-limits
                         :write-batch-max-bytes batch-max-bytes
                         :remaining (- byte-budget reserved)})))
      (when-not (<= (rmw-allowance rmw-max) batch-max-bytes)
        (throw (ex-info "RMW allowance does not fit the batch byte cap"
                        {:error :txlog/write-protocol-limits
                         :wal-rmw-max-bytes rmw-max
                         :write-batch-max-bytes batch-max-bytes})))
      (assoc default-limits
             :max-requests max-requests
             :byte-budget byte-budget
             :shared-reserved reserved
             :request-budget (- byte-budget reserved)
             :batch-limit batch-limit
             :batch-max-bytes batch-max-bytes
             :waiter-limit waiter-limit
             :rmw-allowance-bytes rmw-max))))
