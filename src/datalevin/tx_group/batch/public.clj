;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.public
  "Opt-in public KV routing over the independent collector. Reads retain KVLMDB's
  ordinary native forwarders. This first migration supports a declared catalog
  of ordinary and list DBIs, typed unconditional writes and once-only write bodies."
  (:require [clojure.java.io :as io]
            [datalevin.bits :as bits]
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
            [datalevin.validate :as validate])
  (:import [datalevin.lmdb KVTxData]
           [java.nio ByteBuffer BufferOverflowException]
           [java.util ArrayList]
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

(defn- encode
  "Charge scratch, growth overlap and the detached byte array before allocation.
  Nippy writes into the bounded buffer; no unbounded serialized value is built."
  ^bytes [charge! value type key?]
  ;; String/symbol encoders create a UTF-8 array before copying into the buffer.
  ;; Reserve its worst-case size first, including tuple elements recursively.
  (letfn [(charge-text! [value type]
            (cond
              (#{:string :keyword :symbol :attr} type)
              (charge! (charge/array-bytes 1 (* 3 (long (count (str value))))))
              (vector? type)
              (doseq [[element element-type]
                      (map vector value (if (= 1 (count type))
                                          (repeat (first type)) type))]
                (charge-text! element element-type))))]
    (charge-text! value type))
  (loop [capacity (if key? c/+max-key-size+ 256)]
    (charge! (+ charge/buffer-wrapper (charge/array-bytes 1 capacity)))
    (let [buffer (ByteBuffer/allocate (int capacity))
          encoded? (try (bits/put-buffer buffer
                                        (if (instance? ByteBuffer value)
                                          (.duplicate ^ByteBuffer value) value)
                                        type) true
                        (catch BufferOverflowException _ false))]
      (if encoded?
        (let [size (.position buffer)]
          (charge! (charge/array-bytes 1 size))
          (java.util.Arrays/copyOf (.array buffer) size))
        (if key?
          (throw (ex-info "Encoded key exceeds native key size"
                          {:error :kv/invalid-encoded-size :outcome :not-committed}))
          (recur (Math/multiplyExact (long capacity) 2)))))))

(defn- prepare-rows
  [raw charge! dbi-name txs kt vt]
  (charge! charge/vector-wrapper)
  (let [rows (ArrayList.)]
    (doseq [input txs]
      (let [^KVTxData tx (if dbi-name
                           (if (instance? KVTxData input) input (l/->kv-tx-data input kt vt))
                           (l/->kv-tx-data input))
            name (or dbi-name (.-dbi-name tx))
            options (i/dbi-opts raw name)]
        (when-not (and (some? options) (not= name c/kv-info)
                       (#{:put :del :put-list :del-list :clear} (.-op tx))
                       (empty? (.-flags tx))
                       (or (#{:put :del :clear} (.-op tx))
                           (some #{:dupsort} (:flags options))))
          (unsupported! :transaction-row))
        (if (= :clear (.-op tx))
          (do
            (charge! (+ charge/encoded-row-descriptor
                        (charge/carriers-bytes (inc (.size rows)))))
            (.add rows (l/kv-tx :clear name nil nil :raw :raw)))
          (do
            (validate/validate-kv-tx-data tx (:validate-data? options))
            (let [key (encode charge! (.-k tx) (or (.-kt tx) :data) true)
                  list? (#{:put-list :del-list} (.-op tx))
                  values (if list? (.-v tx) [(.-v tx)])]
              ;; Freeze one physical effect per duplicate. Recovery never needs to
              ;; deserialize a caller's collection or re-evaluate a write body.
              (doseq [value values]
                (charge! (+ charge/encoded-row-descriptor
                            (charge/carriers-bytes (inc (.size rows)))))
                (let [encoded (when-not (= :del (.-op tx))
                                (encode charge! value (or (.-vt tx) :data)
                                        (boolean (some #{:dupsort} (:flags options)))))
                      op (if (= :put-list (.-op tx)) :put (.-op tx))]
                  (when (= :del-list op) (charge! charge/vector-wrapper))
                  (.add rows (l/kv-tx op name key
                                     (if (= :del-list op) [encoded] encoded) :raw :raw)))))))))
    rows))

(defn- prepare-blind [raw state allowance name txs kt vt]
  (let [charge! (local-charge allowance)
        rows (prepare-rows raw charge! name txs kt vt)
        body (when (and state (pos? (.size ^ArrayList rows)))
               (let [estimate (reduce (fn [total ^KVTxData row]
                                        (+ total charge/encoded-row-descriptor
                                           (* 3 (count (.-dbi-name row)))
                                           (if (.-k row) (alength ^bytes (.-k row)) 0)
                                           (if (.-v row) (alength ^bytes (if (= :del-list (.-op row))
                                                                        (first (.-v row)) (.-v row))) 0)))
                                      charge/buffer-wrapper rows)]
                 (charge! estimate)
                 (let [encoded (wal/prepare-append-body rows {})]
                   (charge! (max 0 (- (alength ^bytes encoded) estimate)))
                   encoded)))]
    {:rows rows :wal-body body :result :transacted}))

(defn- blind-allowance
  "Declare known primitive payloads without encoding them. Unknown serialized
  values reserve the configured allowance, then use the same bounded encoder."
  [txs name kt vt fallback]
  (letfn [(size [value type]
            (or (bits/type-size type)
                (case type
                  :string (inc (* 3 (long (count value))))
                  :bytes (inc (long (alength ^bytes value)))
                  :raw (if (bytes? value) (long (alength ^bytes value))
                           (when (instance? ByteBuffer value) (.remaining ^ByteBuffer value)))
                  nil)))]
    (if (instance? java.util.Collection txs)
      (loop [remaining (seq txs) payload 0 rows 0]
        (if-let [input (first remaining)]
          (let [^KVTxData tx (if name
                               (if (instance? KVTxData input) input (l/->kv-tx-data input kt vt))
                               (l/->kv-tx-data input))
                key-size (size (.-k tx) (or (.-kt tx) :data))
                val-size (cond
                           (= :del (.-op tx)) 0
                           (#{:put-list :del-list} (.-op tx)) nil
                           :else (size (.-v tx) (or (.-vt tx) :data)))]
            (if (and key-size val-size)
              (recur (next remaining)
                     (Math/addExact (long payload) (+ (long key-size) (long val-size)))
                     (inc rows))
              fallback))
          (charge/blind-allowance {:declared-bytes (* 4 payload)
                                  :row-capacity rows :scratch-bytes (+ 1024 (* 4 payload))})))
      fallback)))

(defn- run-body [collector body opts]
  (let [timeout (l/explicit-transaction-timeout-ms-from-option
                 (l/explicit-transaction-timeout-option opts))]
    (batch/submit!
     collector
     {:context (:context opts)
      :op (fn [tx]
            (let [watchdog (l/start-explicit-transaction-watchdog! timeout)]
              (try
                (let [result (body (kv/wrap-lmdb tx))]
                  (l/cancel-explicit-transaction-watchdog! watchdog)
                  (l/assert-explicit-transaction-live! watchdog)
                  result)
                (catch Throwable t (l/throw-explicit-transaction-failure! watchdog t))
                (finally (l/cancel-explicit-transaction-watchdog! watchdog)))))})))

(defn open!
  "Open public independent KV, or attach an alias to its single native runtime.
  Open options are persisted in the protocol marker and frozen for aliases.
  Existing compatibility databases require explicit migration outside this API."
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
                                                          name txs kt vt))}))
          {:keys [raw wal-state snapshot! snapshot-state snapshot-error]} (env/resources environment)
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
