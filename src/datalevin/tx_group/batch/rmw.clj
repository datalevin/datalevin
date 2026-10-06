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
            [datalevin.txlog.codec :as codec]
            [datalevin.tx-group.phase :as phase])
  (:import [datalevin.cpp Util$DTLVException]
           [datalevin.lmdb DatomKVTxData KVTxData]
           [datalevin.utl RowRegions]
           [java.io IOException]
           [java.util ArrayList Arrays List]))

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

(defn- capture-prepared!
  "Append storage owned by this native batch, including a late storage drain."
  [wdb apply-rows! rows wal-rows {:keys [defer-rows!]} prepared]
  (.append ^RowRegions rows (:rows prepared))
  (.append ^RowRegions wal-rows (:wal-rows prepared))
  (when-not (and defer-rows! (defer-rows! wdb prepared))
    (apply-rows! (:rows prepared))))

(defn- capture!
  "Freeze writes in the batch transaction, deferring eligible native tails.
   Retain a failure even
   when the body catches it, so no failed batch can reach WAL or native commit."
  [raw wdb apply-rows! descriptor rows wal-rows valid? failure
   {:keys [check-row! prepare-rows! allow-flags? prepared-owned? charge-fn prepared-request?
           application-error!]
    :or {charge-fn charge!} :as opts} dbi-name txs kt vt]
  (try
    (l/write-txn wdb)
    (when-not @valid?
      (throw (ex-info "Write body has finished"
                      {:error :txlog/transaction-view-invalidated :retryable? false})))
    (when-let [t @failure] (throw t))
    (when-let [flush! (:storage-flush! opts)] (flush!))
    (let [prepared (if prepare-rows!
                     (try
                       (prepare-rows! raw descriptor dbi-name txs kt vt)
                       (catch Throwable t
                         ;; Encoding/validation failed before native application.
                         ;; Classify it just like a rejected user body, retaining
                         ;; the cancellation even if that body catches the error.
                         (if application-error!
                           (application-error! t)
                           (throw t))))
                     txs)
          txs (if prepared-request? (:rows prepared) prepared)
          _ (when (and prepared-request? (not prepared-owned?))
              (.append ^RowRegions wal-rows (:wal-rows prepared)))
          dbi-name (when-not prepare-rows! dbi-name)]
    (if (and prepared-request? prepared-owned?)
      ;; Public preparation already validates and owns these native rows.
      ;; Apply the frozen region directly; only the private validation opener
      ;; needs the copy/charge path below.
      (capture-prepared! wdb apply-rows! rows wal-rows opts prepared)
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
  (let [^List rows (if (and (:prepared-request? opts) (:prepared-owned? opts))
               (RowRegions.) (ArrayList.))
        wal-rows (RowRegions.) valid? (volatile! true)
        aborted? (volatile! false)
        shared-view (when (or (:datalog-publication (batch/context descriptor))
                             (:datalog-conn (batch/context descriptor)))
                      (:datalog-view opts))
        wdb (or (when shared-view @shared-view) (l/mark-write native-db))
        _ (when shared-view (vreset! shared-view wdb))
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
        _ (with-meta wdb (assoc (if shared-view
                                 (assoc (meta wdb)
                                        :native-write-owner (:native-write-owner (meta native-db))
                                        :native-write-rtx (:native-write-rtx (meta native-db)))
                                 (meta wdb))
                               :native-row-capture capture
                               :native-storage-staged!
                               (when-let [staged? (:storage-staged? opts)]
                                 (fn []
                                   (vreset! staged? true)
                                   (vreset! (:storage-target opts)
                                            [wdb descriptor rows wal-rows])))
                               :native-prepare-flush! (:preparation-flush! opts)
                               :native-batch-abort! abort!
                               :native-transaction-failed! (fn [t]
                                                             (when-not @failure
                                                               (vreset! failure t)))
                               :request-context (batch/context descriptor)))
        _ (when-let [view (:active-view opts)] (vreset! view wdb))]
    (when-let [staged? (:storage-staged? opts)] (vreset! staged? false))
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
        (let [wal-body (when (and encode-body (not (:encode-batch? opts))
                                 (pos? (.size rows)))
                         (let [estimate (long (if body-cost (body-cost rows) 0))]
                           (charge-fn descriptor estimate)
                           (let [^bytes body (encode-body (if (:prepared-request? opts) wal-rows rows) {})]
                             (charge-fn descriptor (max 0 (- (alength body) estimate)))
                             body)))]
          (batch/set-data! descriptor
                           (if (:encode-batch? opts)
                             {:rows rows :wal-body wal-body :result result
                              :wal-rows wal-rows
                              :storage-staged? (boolean (some-> (:storage-staged? opts) deref))}
                             {:rows rows :wal-body wal-body :result result}))
          (if (and (.isEmpty rows) (not (some-> (:storage-staged? opts) deref))) 0 1)))
      (finally (vreset! valid? false)
               (when-let [view (:active-view opts)] (vreset! view nil))))))

(defn- encode-members!
  "Serialize consecutive body writes together, preserving prepared KV regions
  and the final Datalog metadata trailer in exactly their native order."
  [b encode-body]
  (let [parts (ArrayList.) rows (volatile! (RowRegions.))
        flush! (fn []
                 (when-not (.isEmpty ^RowRegions @rows)
                   (.add parts (encode-body @rows {}))
                   (vreset! rows (RowRegions.))))]
    (dotimes [idx (batch/batch-count b)]
      (let [d (batch/batch-at b idx)
            data (batch/data d)]
        (when-let [body (:wal-body data)]
          (flush!)
          (.add parts body)
          (batch/set-data! d (assoc data :wal-body nil)))
        (when-let [wal-rows (:wal-rows data)]
          (.append ^RowRegions @rows wal-rows))))
    (flush!)
    (when-not (.isEmpty parts) (codec/combine-commit-row-payloads parts))))

(defn execute!
  "Collect while preparing Datalog writes or applying the native prefix, then
  freeze membership for WAL. Resolved Datalog writes can overlap as a batch;
  other bodies first drain deferred writes. Bodies run once before
  append; every member shares the native transaction and commit outcome."
  [raw wal next-lsn! wake! check-batch! opts batch]
  (let [failure (volatile! nil)
        token (volatile! nil)]
    (phase/phase! :native-start batch)
    (cpp/apply-native-once!
     raw
     (fn [wdb]
       (let [active-view (volatile! nil)
             ready? (volatile! false)
             opts (cond-> (assoc opts :active-view active-view)
                    (:shared-datalog-writer? opts) (assoc :datalog-view (volatile! nil)))
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
                                   (do (phase/phase! :native-resized batch)
                                       (refresh!) (recur true))
                                   (if-let [on-error! (:application-error! opts)]
                                     (on-error! failure) (throw failure)))))))
             tail (volatile! nil)
             storage-target (volatile! nil)
             storage-staged? (volatile! false)
             storage-opts (volatile! nil)
             flush-storage!
             (fn []
               (try
                 (when-let [take-rows! (:storage-rows! opts)]
                   (when-let [txs (take-rows!)]
                     (let [[view descriptor rows wal-rows] @storage-target]
                       ;; The batch owns these carriers after the request view
                       ;; expires. Never re-enter that request's capture.
                       (l/write-txn view)
                       (let [prepared ((:prepare-rows! opts) raw descriptor nil txs
                                       :data :data)]
                         (capture-prepared! view apply-rows! rows wal-rows
                                            @storage-opts prepared)))))
                 (catch Throwable t
                   (let [failure-error
                         (try
                           (if-let [on-error! (:application-error! opts)]
                             (on-error! t) (throw t))
                           (catch Throwable e e))]
                     (when-not @failure (vreset! failure failure-error))
                     (throw failure-error)))))
             flush-tail! (fn []
                           (when-let [{:keys [rows view]} @tail]
                             ;; Detach before applying: a map resize rebuilds
                             ;; the writer and must not recursively flush.
                             (vreset! tail nil)
                             (let [_ (with-meta view (dissoc (meta view) :native-before-read!))] nil)
                             (try (phase/phase! :native-tail-start batch)
                                  (apply-rows! rows)
                                  (phase/phase! :native-tail-applied batch)
                                  (catch Throwable t
                                    (when-not @failure (vreset! failure t))
                                    (throw t)))))
             opts (cond-> opts
                    (and wal (:shared-datalog-writer? opts))
                    (assoc :defer-rows!
                           (fn [view prepared]
                             (let [preparing? (:datalog-prepare? (:request-context (meta view)))
                                   ;; Custom payload/index operations allocate IDs
                                   ;; and resolve values through native reads.
                                   ;; Their raw write scope must stay eager.
                                   eligible? (and preparing? (not l/*raw-kv?*)
                                                  (not-any? #(if (instance? KVTxData %)
                                                               (seq (.-flags ^KVTxData %))
                                                               (.-no-overwrite? ^DatomKVTxData %))
                                                            (:rows prepared)))]
                               (cond
                                 eligible?
                                 (let [rows (or (:rows @tail) (RowRegions.))]
                                   (.append ^RowRegions rows (:rows prepared))
                                   (vreset! tail {:rows rows :view view :prepared? true
                                                  :large? (or (:large? @tail)
                                                              (> (long (:native-tail-bytes prepared 0)) 4096))})
                                   true)
                                 :else
                                 (do (flush-tail!)
                                     (when (and (not l/*raw-kv?*)
                                                (> (long (:native-tail-bytes prepared 0)) 4096))
                                       (vreset! tail {:rows (:rows prepared) :view view})
                                       (let [_ (with-meta view (assoc (meta view) :native-before-read! flush-tail!))] nil)
                                       true))))))
                    (:shared-datalog-writer? opts)
                    (assoc :preparation-flush!
                           (fn []
                             (flush-storage!)
                             (flush-tail!)
                             ;; User code can also issue KV writes followed by
                             ;; ordinary native reads. Resume direct application
                             ;; for this body; the next resolver gets a fresh scope.
                             (when-let [view @active-view]
                               (let [_ (with-meta view
                                         (update (meta view) :request-context
                                                 dissoc :datalog-prepare?))] nil)))))
             opts (if (:storage-rows! opts)
                    (assoc opts :storage-target storage-target
                           :storage-staged? storage-staged?
                           :storage-flush! flush-storage!) opts)
             _ (vreset! storage-opts opts)
             apply-member! (fn [idx]
                             (batch/check-preparation! batch)
                             (let [d (batch/batch-at batch idx)]
                               ;; The Datalog resolver carries its own existing
                               ;; transaction indexes across prepared requests.
                               ;; Arbitrary bodies and KV writes need native state.
                               (when-not (and (:datalog-prepare? (batch/context d))
                                              (or (nil? @tail) (:prepared? @tail)))
                                 (flush-storage!)
                                 (flush-tail!))
                               (when-let [before! (:before-body! opts)] (before! d))
                               (if (batch/op d)
                                 (do
                                   ;; Recovery/floor readiness belongs to this
                                   ;; owned transaction, rather than each body.
                                   (when-not @ready?
                                     (when-let [ensure! (:ensure-body-ready! opts)]
                                       (try (ensure! wdb)
                                            (catch Throwable t
                                              (if (clean-body-failure? t)
                                                (batch/cancel-before-dispatch! t)
                                                (throw t)))))
                                     (vreset! ready? true))
                                   (run-member! raw wdb apply-rows! batch d failure opts))
                                 (let [rows (:rows (batch/data d))]
                                   (apply-rows! rows)
                                   (if (seq rows) 1 0)))))
             [^long weight ^long suffix-start]
             (loop [start (long 0)
                    end (long (if (:collect-prepared-prefix? opts)
                                (max 1 (dec (long (batch/batch-count batch))))
                                (min 1 (long (batch/batch-count batch)))))
                    weight (long 0)]
               (let [weight (long
                              (loop [idx (long start) weight (long weight)]
                                (if (< idx end)
                                  (recur (inc idx)
                                         (+ weight (long (apply-member! idx))))
                                  weight)))]
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
                     :else (recur end next-end (long weight))))))
             _ (flush-storage!)
             extra (when-let [finish! (:finish-preparation! opts)]
                       (finish! wdb batch))
               last-member (when (seq extra)
                             (batch/batch-at batch (dec (batch/batch-count batch))))
               previous (when last-member (batch/data last-member))
               extra (when (seq extra)
                       ((:prepare-rows! opts) raw last-member nil extra :data :data))
               suffix-start (if extra
                              (do
                                ;; The trailer follows every request, including
                                ;; prepared KV callers sharing this environment.
                                (when (< suffix-start (batch/batch-count batch)) (flush-tail!))
                                (doseq [idx (range suffix-start (batch/batch-count batch))]
                                  (apply-rows! (:rows (batch/data (batch/batch-at batch idx)))))
                                (long (batch/batch-count batch)))
                              suffix-start)
               weight (if (and extra (empty? (:rows previous))) (inc weight) weight)]
           (when extra
             (if-let [pending @tail]
               (let [rows (RowRegions.)]
                 (.append rows (:rows pending))
                 (.append rows (:rows extra))
                 (vreset! tail (assoc pending :rows rows)))
               (apply-rows! (:rows extra)))
             (let [rows (RowRegions.)
                   bodies (ArrayList.)]
               (.append rows (:rows previous))
               (.append rows (:rows extra))
               ;; A metadata trailer is a WAL body after the request's frozen
               ;; writes, and belongs to that same logical WAL record.
               (when-let [body (:wal-body previous)] (.add bodies body))
               (when-not (:encode-batch? opts)
                 (.add bodies ((:encode-body opts) (:wal-rows extra) {})))
               (batch/set-data! last-member
                                (if (:encode-batch? opts)
                                  (let [wal-rows (RowRegions.)]
                                    (when-let [before (:wal-rows previous)]
                                      (.append wal-rows before))
                                    (.append wal-rows (:wal-rows extra))
                                    (assoc previous :rows rows :wal-rows wal-rows))
                                  (assoc previous :rows rows
                                         :wal-body (codec/combine-commit-row-payloads bodies))))))
         (batch/set-accepted-count!
           batch
           (if (:encode-batch? opts)
             (if-let [body (encode-members! batch (:encode-body opts))]
               (let [d (batch/batch-at batch (dec (batch/batch-count batch)))]
                 (batch/set-data! d (assoc (batch/data d) :wal-body body))
                 1)
               0)
             weight))
         (batch/freeze-schedule!
           batch weight
           (or (and (some? @tail)
                    (or (not (:prepared? @tail))
                        (> (batch/batch-count batch) 1)
                        (:large? @tail)
                        (> (.size ^java.util.List (:rows @tail)) 256)))
               (and (< suffix-start (batch/batch-count batch))
                    ;; For tiny prepared suffixes the worker handoff costs more
                    ;; than applying the rows inline. Larger suffixes still overlap.
                    (or (not (:collect-prepared-prefix? opts))
                        (> (long (reduce (fn [total idx]
                                           (if-let [^bytes body (:wal-body (batch/data (batch/batch-at batch idx)))]
                                             (+ (long total) (alength body)) total))
                                         0 (range suffix-start (batch/batch-count batch))))
                           4096))))
           ;; A single large frozen tail also repays a worker handoff.
           (and (some? @tail)
                (or (not (:prepared? @tail)) (:large? @tail)
                    (> (.size ^java.util.List (:rows @tail)) 256))))
         (when (and wal (pos? weight))
           (batch/set-lsn! batch (long (next-lsn!)))
           (batch/refresh-wal-bodies! batch))
         (when check-batch! (check-batch! batch))
         (when-let [before! (:before-append! opts)] (before! batch))
         (batch/begin-dispatch! batch)
         (let [apply-suffix! (fn []
                               (flush-tail!)
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
