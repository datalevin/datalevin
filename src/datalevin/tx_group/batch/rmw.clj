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
  (:import [datalevin.cpp Util$DTLVException]
           [datalevin.lmdb KVTxData]
           [java.io IOException]
           [java.util ArrayList Arrays]))

(defn- clean-body-failure? [^Throwable t]
  (and (instance? Exception t)
       (not (instance? IOException t))
       (not (instance? Util$DTLVException t))
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
                         @failure (throw (or (owner-interruption @failure) @failure))
                         (owner-interruption t) (throw (owner-interruption t))
                         (clean-body-failure? t) (batch/cancel-before-dispatch! t)
                         :else (throw t))))]
        (when-let [t @failure] (throw (or (owner-interruption t) t)))
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
  "Collect while applying the native prefix, then freeze membership for WAL.
  A final prepared suffix can overlap WAL work. Bodies run once before append;
  every member shares the same native transaction and commit outcome."
  [raw wal next-lsn! wake! check-batch! opts batch]
  (let [failure (volatile! nil)
        token (volatile! nil)
        apply-member! (fn [wdb idx]
                        (batch/check-preparation! batch)
                        (let [d (batch/batch-at batch idx)]
                          (if (batch/op d)
                            (run-member! raw wdb batch d failure opts)
                            (do (i/transact-kv raw (encoding/storage-rows (:rows (batch/data d))))
                                1))))]
    (phase/phase! :native-start batch)
    (cpp/apply-native-once!
     raw
     (fn [wdb]
       (let [[weight suffix-start]
             (loop [start 0 end (min 1 (batch/batch-count batch)) weight 0]
               (let [weight (loop [idx start weight weight]
                              (if (< idx end)
                                (recur (inc idx) (+ weight (apply-member! wdb idx)))
                                weight))]
                 (phase/phase! :native-prefix-applied batch)
                 (when (:collect? opts) (batch/collect-ready! batch))
                 (let [next-end (batch/batch-count batch)]
                   (cond
                     (= end next-end) [weight end]
                     ;; These already-prepared writes need no body evaluation.
                     ;; Freeze before append, then apply them alongside WAL I/O.
                     (and wal (every? #(nil? (batch/op (batch/batch-at batch %)))
                                      (range end next-end)))
                     [(+ weight (- next-end end)) end]
                     :else (recur end next-end weight)))))]
         (batch/set-accepted-count! batch weight)
         (batch/freeze-schedule! batch weight (< suffix-start (batch/batch-count batch)))
         (when (and wal (pos? weight))
           (batch/set-lsn! batch (long (next-lsn!)))
           (batch/refresh-wal-bodies! batch))
         (when check-batch! (check-batch! batch))
         (batch/begin-dispatch! batch)
         (let [apply-suffix! (fn []
                               (loop [idx (long suffix-start)]
                                 (when (< idx (batch/batch-count batch))
                                   (i/transact-kv
                                    raw (encoding/storage-rows
                                         (:rows (batch/data (batch/batch-at batch idx)))))
                                   (recur (inc idx))))
                               (phase/phase! :native-applied batch))]
           (if (and wal (pos? weight))
             (vreset! token
                      (if-let [run-policy! (:run-policy! opts)]
                        (run-policy! batch (batch/batch-lsn batch) apply-suffix!)
                        (let [value (executor/append-group! wal batch (batch/batch-lsn batch))
                              durable? (boolean (executor/complete-policy!
                                                 wal value (batch/batch-cutoff batch)))]
                          (batch/record-wal-policy! batch durable?)
                          (when-not durable? (wake!))
                          (phase/phase! :wal-complete batch)
                          (apply-suffix!)
                          value)))
             (apply-suffix!)))
         (when (zero? weight) (i/abort-transact-kv raw))))
     (fn [wdb _]
       (batch/check-serving! (batch/batch-collector batch))
       (when (batch/expired? batch)
         (throw (ex-info "Write deadline expired before native commit"
                         {:error :txlog/write-deadline-exceeded
                          :outcome :not-committed :retryable? false})))
       (when-let [write-metadata! (:write-metadata! opts)]
         (write-metadata! wdb @token))))
    (phase/phase! :native-committed batch)
    (phase/phase! :execution-complete batch)
    (let [values (object-array (batch/batch-count batch))]
      (dotimes [idx (batch/batch-count batch)]
        (aset values idx (:result (batch/data (batch/batch-at batch idx)))))
      values)))
