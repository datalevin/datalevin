;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.public
  "Private prototype opener and preparation helpers used by embedded KV.
  Production routing uses batch.embedded and the existing storage layout."
  (:require [clojure.java.io :as io]
            [datalevin.bits :as bits]
            [datalevin.binding.cpp :as cpp]
            [datalevin.binding.cpp.lifecycle :as lifecycle]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-group.batch.private :as private]
            [datalevin.tx-group.batch.recovery :as recovery]
            [datalevin.tx-state.protocol :as protocol]
            [datalevin.txlog :as wal]
            [datalevin.txlog.codec :as codec]
            [datalevin.validate :as validate])
  (:import [datalevin.lmdb KVTxData]
           [datalevin.kv.encoding OwnedKVTxData]
           [java.nio ByteBuffer BufferOverflowException]
           [org.eclipse.collections.impl.list.mutable FastList]
           [java.util.concurrent.atomic AtomicBoolean]))

(def ^:private open-lock (Object.))
(def ^:private option-keys
  [:wal? :dbis :flags :mapsize :max-readers :max-dbs :spill-opts
   :wal-durability-profile :wal-sync-mode :wal-group-commit :wal-group-commit-ms
   :wal-segment-max-bytes :wal-segment-max-ms :wal-segment-prealloc?
   :wal-segment-prealloc-bytes :wal-segment-prealloc-mode
   :wal-retention-bytes :wal-retention-ms :wal-retained-max-bytes
   :wal-dir :snapshot-dir :snapshot-scheduler? :snapshot-compact? :snapshot-interval-ms
   :snapshot-max-age-ms :snapshot-max-lsn-delta :snapshot-max-log-bytes-delta
   :snapshot-scheduler-poll-ms :snapshot-offpeak-windows
   :snapshot-defer-on-contention? :snapshot-contention-thresholds
   :wal-pending-max-requests :wal-pending-max-bytes :write-batch-size
   :write-batch-max-bytes :wal-rmw-max-bytes :wal-preparation-timeout-ms
   :write-close-timeout-ms])
(def ^:private supported-option-keys (set (conj option-keys :write-mode :db-identity)))

(defn- unsupported! [operation]
  (throw (ex-info "Operation is not yet supported by public independent KV"
                  {:error :txlog/unsupported-public-operation
                   :operation operation :outcome :not-committed :retryable? false})))

(defn- validate-options! [opts]
  (doseq [[key value] opts] (validate/validate-option-mutation key value))
  (when (or (:temp? opts) (:inmemory? opts) (:ha-mode opts) (:wal-shared? opts)
            (:key-compress opts) (:val-compress opts)
            (some #{:inmemory :rdonly-env :nosubdir} (:flags opts)))
    (unsupported! :environment-options))
  (when-not (and (map? (:dbis opts))
                 (every? (fn [[name options]]
                           (and (string? name) (not= name c/kv-info) (map? options)
                                (not (:key-type options)) (not (:value-type options))
                                (every? #{:create :dupsort :counted :prefix-compression}
                                        (:flags options))))
                         (:dbis opts)))
    (unsupported! :catalog))
  (doseq [key [:wal-pending-max-requests :wal-pending-max-bytes :write-batch-size
              :write-batch-max-bytes :wal-rmw-max-bytes :wal-preparation-timeout-ms
              :write-close-timeout-ms]
          :when (contains? opts key)]
    (when-not (pos-int? (get opts key))
      (throw (ex-info "Write limit must be a positive integer"
                      {:error :txlog/write-protocol-limits :option key :value (get opts key)}))))
  (charge/resolve-limits opts))

(defn- local-charge [allowance]
  (let [used (volatile! (long charge/request-control-bundle))]
    (fn [n]
      (let [next (Math/addExact (long @used) (long n))]
        (when (> next (long allowance))
          (throw (ex-info "Encoded request exceeds its reserved allowance"
                          {:error :txlog/pending-budget-exceeded
                           :outcome :not-committed :retryable? false})))
        (vreset! used next)))))

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

(defn- prepare-blind [raw state allowance name txs kt vt]
  (let [charge! (local-charge allowance)
        wal? (some? state)
        ;; Covers the body header/array overhead in addition to per-row bytes.
        _ (when wal? (charge! charge/buffer-wrapper))
        rows (prepare-rows raw charge! name txs kt vt wal?)
        body (when (and wal? (not (.isEmpty rows)))
               (wal/prepare-append-body rows {}))]
    {:rows rows :wal-body body :result :transacted}))

(defn- payload-size [value type]
  (or (bits/type-size type)
      (case type
        :string (inc (* 3 (long (count value))))
        :bytes (inc (long (alength ^bytes value)))
        :raw (if (bytes? value) (long (alength ^bytes value))
                 (when (instance? ByteBuffer value) (.remaining ^ByteBuffer value)))
        nil)))

(defn- blind-allowance
  "Declare known primitive payloads without encoding them. Unknown serialized
  values reserve the configured allowance, then use the same bounded encoder."
  [txs name kt vt fallback]
  (if (instance? java.util.Collection txs)
    (loop [remaining (seq txs) payload 0 rows 0]
      (if-let [input (first remaining)]
        ;; The common named-DBI vector form can be inspected without allocating
        ;; a second KVTxData before admission. Preparation canonicalizes it once.
        (let [named-vector? (and name (vector? input))
              ^KVTxData tx (when-not named-vector?
                             (if name
                               (if (instance? KVTxData input) input (l/->kv-tx-data input kt vt))
                               (l/->kv-tx-data input)))
              op (if named-vector? (nth input 0) (.-op tx))
              key (if named-vector? (nth input 1) (.-k tx))
              value (if named-vector? (nth input 2 nil) (.-v tx))
              key-type (or (if named-vector? kt (.-kt tx)) :data)
              value-type (or (if named-vector? vt (.-vt tx)) :data)
              key-size (payload-size key key-type)
              val-size (cond
                         (= :del op) 0
                         (#{:put-list :del-list} op) nil
                         :else (payload-size value value-type))]
          (if (and key-size val-size)
            (recur (next remaining)
                   (Math/addExact (long payload) (+ (long key-size) (long val-size)))
                   (inc rows))
            fallback))
        (charge/blind-allowance {:declared-bytes (* 4 payload)
                                :row-capacity rows :scratch-bytes (+ 1024 (* 4 payload))})))
    fallback))

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
                       (> projected (recovery/retention-limit options)))
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

(defn open!
  "Private M0/M1 validation opener. Its protocol marker and fixed catalog are
  test scaffolding; production opens the existing native store via conn/open-kv."
  [dir requested]
  (when-not (string? dir) (unsupported! :environment-path))
  (locking open-lock
    (let [dir (.getCanonicalPath (io/file dir))
          marker (protocol/read-write-protocol-marker dir)
          stored (:public-options marker)
          requested (c/canonicalize-wal-opts requested)
          _ (when (some #(not (contains? supported-option-keys %))
                        (keys requested))
              (unsupported! :environment-options))
          supplied (select-keys requested option-keys)
          options (merge {:wal? false :dbis {}} stored supplied)
          _ (when (and marker (not stored)) (unsupported! :existing-private-environment))
          _ (when (and (nil? marker) (.exists (io/file dir "data.mdb")))
              (unsupported! :existing-compatibility-environment))
          _ (when (and stored (some (fn [[key value]] (not= value (get stored key))) supplied))
              (throw (ex-info "Alias options do not match the open environment"
                              {:error :txlog/write-protocol-mismatch :dir dir})))
          _ (validate-options! (merge requested options))
          identity (or (:db-identity marker) (:db-identity requested) (str (random-uuid)))
          _ (when (and (:db-identity requested) (not= identity (:db-identity requested)))
              (throw (ex-info "Database identity does not match"
                              {:error :txlog/database-identity-mismatch :dir dir})))
          options (assoc options :write-mode :independent)
          environment (private/open!
                       (assoc options :dir dir :db-identity identity
                              :protocol-options options
                              :rmw-opts {:prepare-rows! (fn [raw descriptor name txs kt vt]
                                                         (prepare-rows
                                                          raw
                                                          #(batch/charge! descriptor %)
                                                          name txs kt vt false))}))
          {:keys [raw wal-state wake-wal! snapshot! snapshot-state snapshot-error]} (env/resources environment)
          collector (env/collector environment)
          close-actions (or (:close-actions (:independent-control @(i/kv-info raw))) (atom {}))
          closed (AtomicBoolean.)
          close! (fn []
                   (when (Thread/holdsLock (l/write-txn raw))
                     (unsupported! :close-inside-transaction))
                   (when (.compareAndSet closed false true)
                     (swap! close-actions dissoc closed)
                     (env/close! environment)))
          allowance (long (get options :wal-rmw-max-bytes (:rmw-allowance-bytes charge/default-limits)))
          control {:options (assoc options :db-identity identity)
                   :close-actions close-actions
                   :clear-dbi! #(clear-admin! raw wal-state collector wake-wal! options %)
                   :body! #(run-body collector %1 %2)
                   :transact! (fn [name txs kt vt]
                                (batch/check-serving! collector)
                                (if (seq txs)
                                (batch/submit! collector
                                               (let [reserved (blind-allowance txs name kt vt allowance)]
                                                 {:allowance reserved
                                                  :prepare (fn [_] (prepare-blind raw wal-state reserved name txs kt vt))}))
                                  :transacted))
                   :snapshot! (or snapshot! #(unsupported! :snapshot-without-wal))
                   :snapshots #(recovery/available-snapshots (assoc options :dir dir :db-identity identity))
                   :snapshot-state snapshot-state :snapshot-error snapshot-error
                   :scheduler-state (fn []
                                      {:enabled? (boolean (and wal-state (get options :snapshot-scheduler? true)))
                                       :running? (boolean (and wal-state (get options :snapshot-scheduler? true)
                                                               (batch/serving? collector)))
                                       :latest (some-> snapshot-state deref)
                                       :last-error (some-> snapshot-error deref ex-message)})
                   :force! (fn []
                             (when-not wal-state (unsupported! :force-without-wal))
                             (batch/check-serving! collector)
                             (wal/force-through! wal-state (dec (long @(:next-lsn wal-state))) 0))
                   :watermarks (fn []
                                 (if wal-state
                                   {:wal? true :write-mode :independent
                                    :last-appended-lsn (long @(:last-appended-lsn (:sync-manager wal-state)))
                                    :durable-lsn (long @(:last-durable-lsn (:sync-manager wal-state)))
                                    :applied-lsn (batch/published-lsn collector)}
                                   {:wal? false :write-mode :independent}))
                   :open-dbi! (fn [name supplied]
                                (when-not (and (contains? (:dbis options) name)
                                               (every? (fn [[key value]]
                                                         (= value (get (i/dbi-opts raw name) key))) supplied))
                                  (unsupported! :catalog-mutation))
                                (i/get-dbi raw name false))
                   :unsupported! unsupported!}]
      (vswap! (i/kv-info raw) assoc :independent-control control)
      (swap! close-actions assoc closed close!)
      (lifecycle/register-shutdown-close! raw #(doseq [release! (vals @close-actions)] (release!)))
      (kv/->KVLMDB raw {:close! close! :closed? closed}))))
