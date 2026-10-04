;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.rmw
  "Native RMW execution. Bodies receive the ordinary writable LMDB handle.
   Only writes are captured for WAL. The whole batch commits or aborts;
   reads use the ordinary native transaction."
  (:require [datalevin.binding.cpp :as cpp]
            [datalevin.interface :as i]
            [datalevin.kv.encoding :as encoding]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.phase :as phase])
  (:import [datalevin.lmdb KVTxData]
           [java.io IOException]
           [java.util ArrayList Arrays]))

(defn any-body? [batch]
  (boolean (some #(batch/op (batch/batch-at batch %))
                 (range (batch/batch-count batch)))))

(defn- clean-body-failure? [^Throwable t]
  (and (instance? Exception t)
       (not (instance? IOException t))
       (not (instance? InterruptedException t))
       (not (batch/pre-dispatch-cancellation? t))
       (not (:resized (ex-data t)))
       (not (#{:txlog/runtime-fenced :txlog/runtime-closed
               :txlog/write-deadline-exceeded :txlog/write-interrupted}
             (:error (ex-data t))))
       (or (nil? (ex-cause t)) (clean-body-failure? (ex-cause t)))))

(defn- owner-interruption
  [^Throwable t]
  (if (instance? InterruptedException t)
    t
    (when-let [cause (ex-cause t)] (owner-interruption cause))))

(defn- charge! [descriptor n]
  (try (batch/charge! descriptor n)
       (catch Throwable t
         (if (= :txlog/pending-budget-exceeded (:error (ex-data t)))
           (batch/cancel-before-dispatch! t)
           (throw t)))))

(defn- owned ^bytes [descriptor ^bytes value]
  (charge! descriptor (charge/array-bytes 1 (alength value)))
  (Arrays/copyOf value (alength value)))

(defn- capture!
  "Freeze and apply writes in the batch transaction. Retain a failure even
   when the body catches it, so no failed batch can reach WAL or native commit."
  [raw wdb descriptor rows valid? failure check-row! dbi-name txs kt vt]
  (try
    (l/write-txn wdb)
    (when-not @valid?
      (throw (ex-info "Write body has finished"
                      {:error :txlog/transaction-view-invalidated :retryable? false})))
    (when-let [t @failure] (throw t))
    (doseq [row txs]
      (let [record? (instance? KVTxData row)
            op (if record? (.-op ^KVTxData row) (nth row 0))
            name (or dbi-name (if record? (.-dbi-name ^KVTxData row) (nth row 1)))
            k (if record? (.-k ^KVTxData row) (nth row (if dbi-name 1 2)))
            v (when (= op :put)
                (if record? (.-v ^KVTxData row) (nth row (if dbi-name 2 3))))
            key-type (if record? (.-kt ^KVTxData row)
                         (if dbi-name kt (nth row (if (= op :put) 4 3) :data)))
            val-type (if record? (.-vt ^KVTxData row)
                         (if dbi-name vt (nth row 5 :data)))
            flags (if record? (.-flags ^KVTxData row)
                      (nth row (if dbi-name (if (= op :put) 3 2)
                                   (if (= op :put) 6 4)) nil))]
        (when-not (and (#{:put :del} op) (= key-type :raw)
                       (or (= op :del) (= val-type :raw)) (empty? flags))
          (throw (ex-info "Operation is outside private native RMW scope"
                          {:error :txlog/unsupported-private-operation
                           :outcome :not-committed :retryable? false})))
        (check-row! name op k v)
        (charge! descriptor (+ charge/encoded-row-descriptor charge/vector-wrapper))
        (let [key (owned descriptor k)
              value (when (= op :put) (owned descriptor v))
              forward (l/kv-tx op name key value :raw :raw)]
          (.add ^ArrayList rows forward)
          (i/transact-kv raw (encoding/storage-rows [forward])))))
    :transacted
    (catch Throwable t
      (let [failure-error
            (if (and (= :not-committed (:outcome (ex-data t)))
                     (clean-body-failure? t))
              (try (batch/cancel-before-dispatch! t) (catch Throwable e e))
              t)]
        (when-not @failure (vreset! failure failure-error))
        (throw failure-error)))))

(defn- run-member!
  [raw native-db batch descriptor failure {:keys [check-row! body-cost encode-body]}]
  (charge! descriptor charge/vector-wrapper)
  (let [rows (ArrayList.) valid? (volatile! true)
        wdb (l/mark-write native-db)
        capture (fn [name txs kt vt]
                  (capture! raw wdb descriptor rows valid? failure check-row! name txs kt vt))
        abort! (fn []
                 (let [t (try
                           (batch/cancel-before-dispatch!
                            (ex-info "Batch aborted" {:error :txlog/request-aborted
                                                      :outcome :not-committed
                                                      :retryable? false}))
                           (catch Throwable e e))]
                   (when-not @failure (vreset! failure t))
                   (throw t)))
        _ (with-meta wdb (assoc (meta wdb)
                               :native-row-capture capture
                               :native-batch-abort! abort!
                               :native-transaction-failed! (fn [t]
                                                             (when-not @failure
                                                               (vreset! failure t)))
                               :request-context (batch/context descriptor)))]
    (try
      (let [result (try
                     ((batch/op descriptor) wdb)
                     (catch Throwable t
                       (cond
                         @failure (throw @failure)
                         (owner-interruption t) (throw (owner-interruption t))
                         (clean-body-failure? t) (batch/cancel-before-dispatch! t)
                         :else (throw t))))]
        (when-let [t @failure] (throw t))
        (batch/check-preparation! batch)
        (let [wal-body (when (and encode-body (pos? (.size rows)))
                         (let [estimate (long (body-cost rows))]
                           (charge! descriptor estimate)
                           (let [^bytes body (encode-body rows {})]
                             (charge! descriptor (max 0 (- (alength body) estimate)))
                             body)))]
          (batch/set-data! descriptor {:rows rows :wal-body wal-body :result result})
          (if (.isEmpty rows) 0 1)))
      (finally (vreset! valid? false)))))

(defn execute!
  "Run the sealed batch once in LMDB, freeze accepted rows, satisfy WAL policy,
   then commit the same transaction. Never replay a state-dependent body."
  [raw wal next-lsn! wake! check-batch! opts batch]
  (let [values (object-array (batch/batch-count batch))
        failure (volatile! nil)]
    (phase/phase! :native-start batch)
    (cpp/apply-native-once!
     raw
     (fn [wdb]
       (let [weight
             (loop [idx 0 weight 0]
               (if (= idx (batch/batch-count batch))
                 weight
                 (let [d (batch/batch-at batch idx)
                       member-weight
                       (if (batch/op d)
                         (run-member! raw wdb batch d failure opts)
                         (do (i/transact-kv raw (encoding/storage-rows (:rows (batch/data d))))
                             1))
                       data (batch/data d)]
                   (aset values idx (:result data))
                   (recur (inc idx) (+ weight member-weight)))))]
         (batch/set-accepted-count! batch weight)
         (batch/freeze-schedule! batch weight)
         (when (and wal (pos? weight))
           (batch/set-lsn! batch (long (next-lsn!)))
           (batch/refresh-wal-bodies! batch))
         (when check-batch! (check-batch! batch))
         (batch/begin-dispatch! batch)
         (phase/phase! :native-applied batch)
         (when (and wal (pos? weight))
           (let [token (executor/append-group! wal batch (batch/batch-lsn batch))
                 durable? (boolean (executor/complete-policy!
                                     wal token (batch/batch-cutoff batch)))]
             (batch/record-wal-policy! batch durable?)
             (when-not durable? (wake!))
             (phase/phase! :wal-complete batch)))
         (when (zero? weight) (i/abort-transact-kv raw))))
     (fn [_ _]
       (batch/check-serving! (batch/batch-collector batch))
       (when (batch/expired? batch)
         (throw (ex-info "Write deadline expired before native commit"
                         {:error :txlog/write-deadline-exceeded
                          :outcome :not-committed :retryable? false})))))
    (phase/phase! :native-committed batch)
    (phase/phase! :execution-complete batch)
    values))
