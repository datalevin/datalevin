;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.public
  "KV preparation, transaction bodies and standalone admin writes."
  (:require [datalevin.bits :as bits]
            [datalevin.binding.cpp :as cpp]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.wal :as batch-wal]
            [datalevin.txlog :as wal]
            [datalevin.txlog.codec :as codec]
            [datalevin.validate :as validate])
  (:import [datalevin.lmdb KVTxData]
           [datalevin.kv.encoding OwnedKVTxData]
           [java.nio ByteBuffer BufferOverflowException]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- unsupported! [operation]
  (throw (ex-info "Operation is not yet supported by public independent KV"
                  {:error :txlog/unsupported-public-operation
                   :operation operation :outcome :not-committed :retryable? false})))

(defn- charge-text! [charge! value type]
  (cond
    (#{:string :keyword :symbol :attr} type)
    (charge! (charge/array-bytes 1 (* 3 (long (count (str value))))))
    (vector? type)
    (doseq [[element element-type]
            (map vector value (if (= 1 (count type)) (repeat (first type)) type))]
      (charge-text! charge! element element-type))))

(defn- encode
  "Charge scratch, growth overlap and detached bytes before allocation.
  Internal native owners can retain bounded value scratch across requests."
  (^bytes [charge! value type key?]
   (encode charge! value type key? nil))
  (^bytes [charge! value type key? ^objects scratch]
   ;; Text encoders allocate UTF-8 bytes, including tuple elements.
   (charge-text! charge! value type)
   (let [known (or (bits/type-size type)
                   (when (= :raw type)
                     (cond (bytes? value) (alength ^bytes value)
                           (instance? ByteBuffer value)
                           (.remaining ^ByteBuffer value))))
         reusable? (and scratch (not key?) (nil? known))]
     (loop [capacity (long (if known
                            (max 1 (if key?
                                     (min c/+max-key-size+ (long known))
                                     (long known)))
                            (if key? c/+max-key-size+
                              (if-let [^ByteBuffer buffer
                                       (when reusable? (aget scratch 0))]
                                (.capacity buffer) 256))))]
       (charge! (+ charge/buffer-wrapper (charge/array-bytes 1 capacity)))
       (let [^ByteBuffer previous (when reusable? (aget scratch 0))
             buffer (if (and previous (= capacity (.capacity previous)))
                      (.clear previous) (ByteBuffer/allocate (int capacity)))
             ;; Only the serialized native owner supplies scratch. Large
             ;; outliers do not leave arbitrarily large retained buffers.
             _ (when (and reusable? (<= capacity 65536))
                 (aset scratch 0 buffer))
             encoded? (try
                        (bits/put-buffer buffer
                                         (if (instance? ByteBuffer value)
                                           (.duplicate ^ByteBuffer value) value)
                                         type)
                        true
                        (catch BufferOverflowException _ false))]
         (if encoded?
           (let [size (.position buffer)]
             (if (and (not reusable?) (= size capacity))
               ;; Exact-size private buffers already own their bytes. Reused
               ;; scratch must always detach, even when the value fills it.
               (.array buffer)
               (do (charge! (charge/array-bytes 1 size))
                   (java.util.Arrays/copyOf (.array buffer) size))))
           (if key?
             (throw (ex-info "Encoded key exceeds native key size"
                             {:error :kv/invalid-encoded-size
                              :outcome :not-committed}))
             (recur (Math/multiplyExact (long capacity) 2)))))))))

(defn- add-prepared-row!
  [^FastList rows charge! name op ^bytes key ^bytes value wal? flags]
  (charge! (+ charge/encoded-row-descriptor
              (charge/carriers-bytes (inc (.size rows)))))
  (when wal?
    ;; Raw, flag-free physical rows need at most 128 bytes of WAL overhead,
    ;; plus the UTF-8 name and payload. Reserve while those lengths are handy,
    ;; before the WAL encoder allocates its owned body; no second row walk.
    (charge! (+ charge/encoded-row-descriptor (* 3 (count name))
                (alength key) (long (if value (alength value) 0)))))
  (when (= :del-list op) (charge! charge/vector-wrapper))
  (.add rows (l/kv-tx op name key (if (= :del-list op) [value] value) :raw :raw flags)))

(defn prepare-rows
  "Freeze typed KV input for native application and WAL; existing stores allow
  internal DBIs and native flags, whose successful effects are logged by RMW."
  (^FastList [raw charge! dbi-name txs kt vt wal?]
   (prepare-rows raw charge! dbi-name txs kt vt wal? false))
  (^FastList [raw charge! dbi-name txs kt vt wal? existing?]
   (prepare-rows raw charge! dbi-name txs kt vt wal? existing? nil))
  (^FastList [raw charge! dbi-name txs kt vt wal? existing? ^java.util.List log-rows]
   (prepare-rows raw charge! dbi-name txs kt vt wal? existing? log-rows nil))
  (^FastList [raw charge! dbi-name txs kt vt wal? existing? ^java.util.List log-rows scratch]
   (charge! charge/vector-wrapper)
   (let [rows (FastList. 0)
         named-options (when dbi-name (i/dbi-opts raw dbi-name))]
     (doseq [input txs]
       (let [^KVTxData tx (if dbi-name
                            (if (instance? KVTxData input) input (l/->kv-tx-data input kt vt))
                            (l/->kv-tx-data input))
             name (or dbi-name (.-dbi-name tx))
             options (if (and existing? (= name c/kv-info)) {}
                         (if dbi-name named-options (i/dbi-opts raw name)))
             op (.-op tx)
             list? (#{:put-list :del-list} op)
             duplicates? (boolean (some #{:dupsort} (:flags options)))]
         (when-not (and (some? options) (or existing? (not= name c/kv-info))
                        (#{:put :del :put-list :del-list} op)
                        (or existing? (empty? (.-flags tx)))
                        (or (not list?) duplicates?))
           (unsupported! :transaction-row))
         (validate/validate-kv-tx-data tx (:validate-data? options))
         (let [key-type (or (.-kt tx) :data)
               key (encode charge! (.-k tx) key-type true)
               value-type (or (.-vt tx) :data)]
           (if list?
             (do
               (doseq [value (.-v tx)]
                 (add-prepared-row! rows charge! name (if (= :put-list op) :put op)
                                    key (encode charge! value value-type duplicates? scratch) wal? nil))
               (when log-rows
                 (.add log-rows (OwnedKVTxData. op name key
                                               (encode charge! (.-v tx) :data false scratch)
                                               key-type value-type))))
             (let [value (when-not (= :del op)
                           (encode charge! (.-v tx) value-type duplicates? scratch))]
               (add-prepared-row! rows charge! name op key value wal? (.-flags tx))
               (when log-rows
                 (.add log-rows (OwnedKVTxData. op name key value key-type value-type))))))))
     rows)))

(defn run-body [collector body opts]
  (let [timeout (l/explicit-transaction-timeout-ms-from-option
                 (l/explicit-transaction-timeout-option opts))]
    (batch/submit!
     collector
     {:context (:context opts)
      :op (fn [tx]
            (let [watchdog (l/start-explicit-transaction-watchdog! timeout)]
              (try
                ;; This writer belongs to an already-open environment. Opener
                ;; cleanup would close that environment if floor validation
                ;; fails; the native batch owner must abort only its writer.
                (when-not (:ready? opts) (kv/ensure-txlog-ready! tx))
                (let [result (body (kv/->KVLMDB tx nil))]
                  (l/cancel-explicit-transaction-watchdog! watchdog)
                  (l/assert-explicit-transaction-live! watchdog)
                  result)
                (catch Throwable t (l/throw-explicit-transaction-failure! watchdog t))
                (finally (l/cancel-explicit-transaction-watchdog! watchdog)))))})))

(defn clear-admin!
  "Serialize with native writers, then use the existing standalone clear txn.
  Its WAL record preserves the admin effect on snapshot-based recovery; this
  operation is never submitted as a data request or run in a user transaction."
  [raw state collector wake! options name]
  (when (Thread/holdsLock (l/write-txn raw))
    (unsupported! :admin-inside-transaction))
  (locking (l/write-txn raw)
    (batch/check-serving! collector)
    (when-not (and (string? name) (not= name c/kv-info) (i/dbi-opts raw name))
      (unsupported! :unknown-dbi))
    ;; A native DBI name fits in a native key. One small admin record uses the
    ;; existing serialized WAL workspace; no caller graph or batch is retained.
    (when (> (count name) c/+max-key-size+) (unsupported! :dbi-name))
    (i/get-dbi raw name false)
    (let [lsn (when state (long @(:next-lsn state)))
          status (volatile! nil)
          append-token (volatile! nil)
          native? (volatile! false)]
      (try
        (when state
          (let [body (wal/prepare-append-body [(l/kv-tx :clear name nil nil :raw :raw)] {})
                slack (if (:segment-prealloc? state)
                        (* 2 (long (:segment-prealloc-bytes state))) 0)
                projected (+ (long @(:retention-total-bytes state)) slack
                             (alength ^bytes body) codec/record-header-size)]
            (when (and (not= false (:check-retention? options))
                       (> projected (batch-wal/retention-limit options)))
              (throw (ex-info "Required WAL history fills the hard retention limit"
                              {:error :txlog/retention-backpressure
                               :outcome :not-committed :retryable? true})))
            (let [token (wal/begin-prepared-group! state lsn (object-array [body]))]
              (vreset! append-token token)
              (vreset! status :appended)
              (if (wal/finish-prepared-group! state token 0)
                (vreset! status :durable)
                (when wake! (wake!))))))
        (vreset! native? true)
        (cpp/apply-native-range!
          raw
          (fn [wdb] ((cpp/prepared-row-applier wdb) [(l/kv-tx :clear name nil nil :raw :raw)]))
          (fn [wdb _]
            (when-let [write! (:write-metadata! options)] (write! wdb @append-token))))
        (when-let [committed! (:committed! options)] (committed! @append-token))
        (when state (batch/publish-admin-prefix! collector lsn))
        nil
        (catch Throwable t
          (loop [cause t]
            (when cause
              (if (instance? InterruptedException cause)
                (.interrupt (Thread/currentThread))
                (recur (ex-cause cause)))))
          (let [error (if (and @native? state)
                        (ex-info "Standalone clear failed after WAL policy completion"
                                 {:error (if (= :durable @status)
                                           :txlog/write-committed :txlog/write-indeterminate)
                                  :outcome (if (= :durable @status) :committed :indeterminate)
                                  :wal-status @status :lsn lsn :retryable? false} t)
                        t)]
            (when (or @native? @status
                      (not= :not-committed (:outcome (ex-data t))))
              (batch/fence! collector error))
            (throw error)))))))
