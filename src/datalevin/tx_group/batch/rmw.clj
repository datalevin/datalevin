;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.rmw
  "Native RMW execution. Bodies receive the ordinary writable LMDB handle.
   Only writes are captured for WAL. The whole batch commits or aborts;
   reads use the ordinary native transaction."
  (:require [datalevin.binding.cpp :as cpp]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.phase :as phase])
  (:import [datalevin.cpp Util$DTLVException]
           [datalevin.lmdb KVTxData]
           [java.io IOException]
           [java.util ArrayList Arrays]))

(defn clean-body-failure? [^Throwable t]
  (or (and (l/explicit-transaction-timeout-error? t)
           (instance? InterruptedException (ex-cause t)))
      (and (instance? Exception t)
       (not (instance? IOException t))
       (not (instance? Util$DTLVException t))
       (not (instance? InterruptedException t))
       (not (batch/pre-dispatch-cancellation? t))
       (not (:resized (ex-data t)))
       (not (#{:txlog/runtime-fenced :txlog/runtime-closed
               :txlog/write-deadline-exceeded :txlog/write-interrupted}
             (:error (ex-data t))))
       (or (nil? (ex-cause t)) (clean-body-failure? (ex-cause t))))))

(defn- owner-interruption
  [^Throwable t]
  (when-not (l/explicit-transaction-timeout-error? t)
    (if (instance? InterruptedException t)
      t
      (when-let [cause (ex-cause t)] (owner-interruption cause)))))

(defn- charge! [descriptor n]
  (try (batch/charge! descriptor n)
       (catch Throwable t
         (if (= :txlog/pending-budget-exceeded (:error (ex-data t)))
           (batch/cancel-before-dispatch! t)
           (throw t)))))

(defn- owned ^bytes [charge-fn descriptor ^bytes value]
  (charge-fn descriptor (charge/array-bytes 1 (alength value)))
  (Arrays/copyOf value (alength value)))

(defn- capture!
  "Freeze and apply writes in the batch transaction. Retain a failure even
   when the body catches it, so no failed batch can reach WAL or native commit."
  [raw wdb apply-rows! descriptor rows wal-rows valid? failure
   {:keys [check-row! prepare-rows! allow-flags? prepared-owned? charge-fn prepared-request?]
    :or {charge-fn charge!}} dbi-name txs kt vt]
  (try
    (l/write-txn wdb)
    (when-not @valid?
      (throw (ex-info "Write body has finished"
                      {:error :txlog/transaction-view-invalidated :retryable? false})))
    (when-let [t @failure] (throw t))
    (let [prepared (if prepare-rows! (prepare-rows! raw descriptor dbi-name txs kt vt) txs)
          txs (if prepared-request? (:rows prepared) prepared)
          _ (when prepared-request? (.addAll ^ArrayList wal-rows ^java.util.Collection (:wal-rows prepared)))
          dbi-name (when-not prepare-rows! dbi-name)]
    (if (and prepared-request? prepared-owned?)
      ;; Public preparation already validates and owns these native rows.
      ;; Apply the frozen region directly; only the private validation opener
      ;; needs the copy/charge path below.
      (do (.addAll ^ArrayList rows ^java.util.Collection txs)
          (apply-rows! txs))
    (doseq [row txs]
      (let [record? (instance? KVTxData row)
            op (if record? (.-op ^KVTxData row) (nth row 0))
            name (or dbi-name (if record? (.-dbi-name ^KVTxData row) (nth row 1)))
            k (if record? (.-k ^KVTxData row) (nth row (if dbi-name 1 2)))
            v (when (#{:put :del-list} op)
                (if record? (.-v ^KVTxData row) (nth row (if dbi-name 2 3))))
            key-type (if record? (.-kt ^KVTxData row)
                         (if dbi-name kt (nth row (if (= op :put) 4 3) :data)))
            val-type (if record? (.-vt ^KVTxData row)
                         (if dbi-name vt (nth row 5 :data)))
            flags (if record? (.-flags ^KVTxData row)
                      (nth row (if dbi-name (if (= op :put) 3 2)
                                   (if (= op :put) 6 4)) nil))]
        (when-not (and (or (#{:put :del} op)
                           (and prepare-rows! (= :del-list op) (= 1 (count v)))) (= key-type :raw)
                       (or (= op :del) (= val-type :raw)) (or allow-flags? (empty? flags)))
          (throw (ex-info "Operation is outside private native RMW scope"
                          {:error :txlog/unsupported-private-operation
                           :outcome :not-committed :retryable? false})))
        (check-row! name op k v)
        (charge-fn descriptor (+ charge/encoded-row-descriptor charge/vector-wrapper))
        (let [key (when k (if prepared-owned? k (owned charge-fn descriptor k)))
              value (case op
                      :put (if prepared-owned? v (owned charge-fn descriptor v))
                      :del-list (if prepared-owned? v
                                    (do (charge-fn descriptor charge/vector-wrapper)
                                        [(owned charge-fn descriptor (first v))]))
                      nil)
              forward (l/kv-tx op name key value :raw :raw)]
          (.add ^ArrayList rows forward)
          (apply-rows! [(if (seq flags) (l/kv-tx op name key value :raw :raw flags) forward)]))))))
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
  [raw native-db apply-rows! batch descriptor failure
   {:keys [body-cost encode-body charge-fn] :or {charge-fn charge!} :as opts}]
  (charge-fn descriptor charge/vector-wrapper)
  (let [rows (ArrayList.) wal-rows (ArrayList.) valid? (volatile! true)
        aborted? (volatile! false)
        wdb (l/mark-write native-db)
        capture (fn [name txs kt vt]
                  (capture! raw wdb apply-rows! descriptor rows wal-rows valid? failure opts name txs kt vt))
        abort! (fn []
                 (vreset! aborted? true)
                 (let [t (try
                           (batch/cancel-before-dispatch!
                            (ex-info "Batch aborted" {:error :txlog/request-aborted
                                                      :outcome :not-committed
                                                      :retryable? false}))
                           (catch Throwable e e))]
                   (when-not @failure (vreset! failure t))
                   (when-not (:abort-returns? opts) (throw t))))
        _ (with-meta wdb (assoc (meta wdb)
                               :native-row-capture capture
                               :native-batch-abort! abort!
                               :native-transaction-failed! (fn [t]
                                                             (when-not @failure
                                                               (vreset! failure t)))
                               :request-context (batch/context descriptor)))
        _ (when-let [view (:active-view opts)] (vreset! view wdb))]
    (try
      (let [result (try
                     ((batch/op descriptor) wdb)
                     (catch Throwable t
                       (cond
                         @failure (throw (or (owner-interruption @failure) @failure))
                         (owner-interruption t) (throw (owner-interruption t))
                         (clean-body-failure? t) (batch/cancel-before-dispatch! t)
                         :else (throw t))))]
        (when-let [t @failure]
          (if (and @aborted? (:abort-returns? opts)
                   (or (batch/pre-dispatch-cancellation? t) (clean-body-failure? t)))
            (batch/cancel-before-dispatch!
              (ex-info "Batch aborted" {:error :txlog/request-aborted
                                        :outcome :not-committed :retryable? false} t))
            (throw (or (owner-interruption t) t))))
        (batch/check-preparation! batch)
        (let [wal-body (when (and encode-body (pos? (.size rows)))
                         (let [estimate (long (if body-cost (body-cost rows) 0))]
                           (charge-fn descriptor estimate)
                           (let [^bytes body (encode-body (if (:prepared-request? opts) wal-rows rows) {})]
                             (charge-fn descriptor (max 0 (- (alength body) estimate)))
                             body)))]
          (batch/set-data! descriptor {:rows rows :wal-body wal-body :result result})
          (if (.isEmpty rows) 0 1)))
      (finally (vreset! valid? false)
               (when-let [view (:active-view opts)] (vreset! view nil))))))

(defn execute!
  "Collect while applying the native prefix, then freeze membership for WAL.
  A final prepared suffix can overlap WAL work. Bodies run once before append;
  every member shares the same native transaction and commit outcome."
  [raw wal next-lsn! wake! check-batch! opts batch]
  (let [failure (volatile! nil)
        token (volatile! nil)]
    (phase/phase! :native-start batch)
    (cpp/apply-native-once!
     raw
     (fn [wdb]
       (let [active-view (volatile! nil)
             opts (assoc opts :active-view active-view)
             history (when (:retry-frozen? opts) (ArrayList.))
             native-apply! (volatile! (cpp/prepared-row-applier wdb))
             refresh! (fn []
                        (doseq [view [wdb @active-view] :when view]
                          (let [_ (with-meta view (assoc (meta view) :native-write-rtx @(l/write-txn raw)))]
                            nil))
                        (vreset! native-apply! (cpp/prepared-row-applier wdb)))
             apply-rows! (fn [rows]
                           (when history (.add history rows))
                           (loop [replay? false]
                             (let [failure (try
                                             (if replay?
                                               (doseq [region history] (@native-apply! region))
                                               (@native-apply! rows))
                                             nil
                                             (catch Throwable t t))]
                               (when failure
                                 (if (and history (:resized (ex-data failure)))
                                   (do (refresh!) (recur true))
                                   (if-let [on-error! (:application-error! opts)]
                                     (on-error! failure) (throw failure)))))))
             apply-member! (fn [idx]
                             (batch/check-preparation! batch)
                             (let [d (batch/batch-at batch idx)]
                               (if (batch/op d)
                                 (run-member! raw wdb apply-rows! batch d failure opts)
                                 (let [rows (:rows (batch/data d))]
                                   (apply-rows! rows)
                                   (if (seq rows) 1 0)))))
             [^long weight ^long suffix-start]
             (loop [start (long 0)
                    end (long (if (:collect-prepared-prefix? opts)
                                (max 1 (dec (long (batch/batch-count batch))))
                                (min 1 (long (batch/batch-count batch)))))
                    weight (long 0)]
               (let [weight (loop [idx (long start) weight (long weight)]
                              (if (< idx end)
                                (recur (inc idx)
                                       (+ weight (long (apply-member! idx))))
                                weight))]
                 (phase/phase! :native-prefix-applied batch)
                 ;; Give already-admitted preparers one scheduling turn before
                 ;; freezing a microsecond native prefix. This is a bounded
                 ;; hint, never a wait for completion or permission to collect
                 ;; an unprepared request. Uncontended writes do not park.
                 (when (and (:collect-prepared-prefix? opts)
                            (batch/callers-preparing? batch))
                   (java.util.concurrent.locks.LockSupport/parkNanos 1000))
                 (let [collected (long (if (:collect? opts) (batch/collect-ready! batch) 0))
                       next-end (long (batch/batch-count batch))]
                   (cond
                     (= end next-end) [weight end]
                     ;; Preserve the old collector's collection during native
                     ;; application. Apply newly collected prefixes, retaining
                     ;; one prepared final member for useful WAL overlap.
                     (and (:collect-prepared-prefix? opts) (pos? collected))
                     (recur end
                            (long (if (and wal
                                           (nil? (batch/op (batch/batch-at batch (dec next-end))))
                                           (not (:state-dependent? (batch/data (batch/batch-at batch (dec next-end))))))
                                    (max end (dec next-end)) next-end)) weight)
                     ;; These already-prepared writes need no body evaluation.
                     ;; Freeze before append, then apply them alongside WAL I/O.
                     (and wal (every? #(let [d (batch/batch-at batch %)]
                                         (and (nil? (batch/op d))
                                              (not (:state-dependent? (batch/data d)))))
                                      (range end next-end)))
                     [(reduce (fn [total idx]
                                (+ (long total)
                                   (if (seq (:rows (batch/data (batch/batch-at batch idx)))) 1 0)))
                              weight (range end next-end)) end]
                     :else (recur end next-end (long weight))))))]
         (batch/set-accepted-count! batch weight)
         (batch/freeze-schedule!
           batch weight
           (and (< suffix-start (batch/batch-count batch))
                ;; For tiny prepared suffixes the worker handoff costs more
                ;; than applying the rows inline. Larger suffixes still overlap.
                (or (not (:collect-prepared-prefix? opts))
                    (> (reduce (fn [total idx]
                                 (if-let [^bytes body (:wal-body (batch/data (batch/batch-at batch idx)))]
                                   (+ (long total) (alength body)) total))
                               0 (range suffix-start (batch/batch-count batch)))
                       4096))))
         (when (and wal (pos? weight))
           (batch/set-lsn! batch (long (next-lsn!)))
           (batch/refresh-wal-bodies! batch))
         (when check-batch! (check-batch! batch))
         (when-let [before! (:before-append! opts)] (before! batch))
         (batch/begin-dispatch! batch)
         (let [apply-suffix! (fn []
                               (loop [idx (long suffix-start)]
                                 (when (< idx (batch/batch-count batch))
                                   (apply-rows! (:rows (batch/data (batch/batch-at batch idx))))
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
    (when-let [committed! (:committed! opts)] (committed! batch @token))
    (phase/phase! :native-committed batch)
    (phase/phase! :execution-complete batch)
    (let [values (object-array (batch/batch-count batch))]
      (dotimes [idx (batch/batch-count batch)]
        (aset values idx (:result (batch/data (batch/batch-at batch idx)))))
      values)))
