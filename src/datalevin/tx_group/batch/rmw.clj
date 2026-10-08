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
            [datalevin.tx-group.batch.wal :as wal]
            [datalevin.txlog.codec :as codec]
            [datalevin.tx-group.phase :as phase])
  (:import [datalevin.cpp Util$DTLVException]
           [datalevin.lmdb DatomKVTxData KVTxData]
           [datalevin.utl DeferredRows RowRegions]
           [java.io IOException]
           [java.util ArrayList Arrays List]
           [java.util.function Supplier]))

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
  [wdb apply-rows! rows wal-rows
   {:keys [defer-rows! capture-context capture-batch]} prepared]
  (.append ^RowRegions rows (:rows prepared))
  (.append ^RowRegions wal-rows (:wal-rows prepared))
  (let [context (or capture-context (l/request-context wdb))
        deferred? (boolean (and defer-rows! (defer-rows! wdb prepared context)))]
    (phase/phase! (if deferred? :native-capture-deferred :native-capture-eager)
                  {:batch capture-batch :rows (count (:rows prepared))
                   :storage-tail? (boolean (:storage-tail? prepared))
                   :preparing? (boolean (:datalog-prepare? context))
                   :raw? (boolean l/*raw-kv?*)})
    (when-not deferred? (apply-rows! (:rows prepared)))))

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

(defn- resolve-member!
  [b descriptor view failure aborted? opts]
  (phase/phase! :resolution-start b)
  (let [result (try
                 ((batch/op descriptor) view)
                 (catch Throwable t
                   (cond
                     @failure (throw (or (owner-interruption @failure) @failure))
                     (owner-interruption t) (throw (owner-interruption t))
                     (clean-body-failure? t) (batch/cancel-before-dispatch! t)
                     :else (throw t))))]
    (phase/phase! :resolution-complete b)
    (when-let [t @failure]
      (if (and @aborted? (:abort-returns? opts)
               (or (batch/pre-dispatch-cancellation? t) (clean-body-failure? t)))
        (batch/cancel-before-dispatch!
          (ex-info "Batch aborted" {:error :txlog/request-aborted
                                    :outcome :not-committed :retryable? false} t))
        (throw (or (owner-interruption t) t))))
    (batch/check-preparation! b)
    result))

(def ^:dynamic *reuse-datalog-member-setup?*
  "Internal comparison switch for member-setup allocation measurements."
  true)

(def ^:dynamic *defer-storage-encoding?*
  "Internal comparison switch for the final storage drain."
  true)

(defn- member-carriers!
  "Allocate owned row regions only when this member captures physical rows."
  [^objects state]
  (when-not (aget state 1)
    (aset state 1 (RowRegions.))
    (aset state 2 (RowRegions.)))
  state)

(defn- datalog-member-setup
  "One callback scope for trusted resolvers in a native batch. Descriptor and
  carrier slots belong to the currently executing member; retained row regions
  themselves are never reset or reused."
  [raw native-db apply-rows! failure opts]
  (let [;; Active descriptor, native rows, WAL rows, optional user-code capture
        ;; validity, and request context. All slots belong to the native owner.
        state (object-array 5)
        valid? (volatile! false)
        aborted? (volatile! false)
        shared-view (:datalog-view opts)
        wdb (or @shared-view (l/mark-write native-db))
        _ (vreset! shared-view wdb)
        capture (fn [name txs kt vt]
                  (member-carriers! state)
                  (capture! raw wdb apply-rows! (aget state 0)
                            (aget state 1) (aget state 2) valid? failure
                            opts name txs kt vt))
        abort! (fn []
                 (vreset! aborted? true)
                 (let [t (try
                           (batch/cancel-before-dispatch!
                             (ex-info "Batch aborted"
                                      {:error :txlog/request-aborted
                                       :outcome :not-committed :retryable? false}))
                           (catch Throwable e e))]
                   (when-not @failure (vreset! failure t))
                   (when-not (:abort-returns? opts) (throw t))))
        callbacks
        {:native-request-context state
         :native-row-capture capture
         :native-storage-staged!
         (when-let [staged? (:storage-staged? opts)]
           (fn []
             (vreset! staged? true)
             ;; A later drain can attach rows to an expired member. Carry its
             ;; identity, rather than allocating two empty row lists now.
             (vreset! (:storage-target opts)
                      [wdb (aget state 0) nil nil state (l/request-context wdb)])))
         :native-prepare-flush!
         (when-let [flush! (:preparation-flush! opts)]
           (fn []
             (flush!)
             ;; Resolution can enter arbitrary user code. Give that code a
             ;; capture with a permanently expiring request scope, so an escaped
             ;; callback cannot revive when the batch starts its next resolver.
             (when (and @valid? (nil? (aget state 3)))
               (member-carriers! state)
               (let [member-valid? (volatile! true)
                     descriptor (aget state 0)
                     rows (aget state 1)
                     wal-rows (aget state 2)
                     capture (fn [name txs kt vt]
                               (capture! raw wdb apply-rows! descriptor rows wal-rows
                                         member-valid? failure opts name txs kt vt))]
                 (aset state 3 member-valid?)
                 (let [_ (with-meta wdb (assoc (meta wdb) :native-row-capture capture))]
                   nil)))))
         :native-batch-abort! abort!
         :native-transaction-failed! (fn [t] (when-not @failure (vreset! failure t)))}]
    {:state state :valid? valid? :aborted? aborted? :view wdb :native-db native-db
     :callbacks callbacks}))

(defn- run-datalog-member!
  [b descriptor failure opts setup]
  ((:charge-fn opts charge!) descriptor charge/vector-wrapper)
  (let [{:keys [state valid? aborted? view callbacks native-db]} setup
        ^objects state state
        context (batch/context descriptor)
        metadata (meta view)
        current-rtx (:native-write-rtx (meta native-db))
        metadata (if (identical? (:native-write-rtx metadata) current-rtx)
                   metadata (assoc metadata :native-write-rtx current-rtx))]
    (aset state 0 descriptor)
    (aset state 1 nil)
    (aset state 2 nil)
    (aset state 3 nil)
    (aset state 4 context)
    (vreset! valid? true)
    (vreset! aborted? false)
    (when-let [staged? (:storage-staged? opts)] (vreset! staged? false))
    ;; Ordinary RMW members may have temporarily installed their own callbacks
    ;; on the shared view. Restore only on that transition, preserving a writer
    ;; refreshed after map growth. Consecutive resolvers update the owned scope
    ;; rather than allocating another native-view metadata map.
    (let [updated (if (identical? (:native-row-capture metadata)
                                 (:native-row-capture callbacks))
                    metadata (merge (dissoc metadata :request-context) callbacks))]
      (when-not (identical? updated (meta view))
        (let [_ (with-meta view updated)] nil)))
    (when-let [active (:active-view opts)] (vreset! active view))
    (try
      (let [result (resolve-member! b descriptor view failure aborted? opts)
            rows (or (aget state 1) [])
            wal-rows (or (aget state 2) [])
            staged? (boolean (some-> (:storage-staged? opts) deref))]
        (batch/set-data! descriptor
                         (batch/datalog-member-data rows wal-rows result staged?))
        (if (or (seq rows) staged?) 1 0))
      (finally
        (vreset! valid? false)
        (when-let [member-valid? (aget state 3)] (vreset! member-valid? false))
        (aset state 0 nil)
        (aset state 1 nil)
        (aset state 2 nil)
        (aset state 3 nil)
        (aset state 4 nil)
        (when-let [active (:active-view opts)] (vreset! active nil))))))

(defn- run-update-member!
  "Updates expose only a value to user code, so need no escaping view scope."
  [raw native-db apply-rows! b descriptor failure opts]
  ((:charge-fn opts charge!) descriptor charge/vector-wrapper)
  (let [rows (RowRegions.)
        wal-rows (RowRegions.)
        defer? (boolean (:update-tail! opts))
        valid? (volatile! true)
        [name txs kt vt] (resolve-member! b descriptor native-db failure
                                        (volatile! false) opts)]
    (try
      (capture! raw native-db (if defer? identity apply-rows!)
                descriptor rows wal-rows valid? failure
                opts name txs kt vt)
      (let [wal-body (when (and (:encode-body opts) (not (:encode-batch? opts)))
                       (let [estimate (long (if-let [cost (:body-cost opts)]
                                              (cost rows) 0))
                             charge-fn (:charge-fn opts charge!)]
                         (charge-fn descriptor estimate)
                         (let [^bytes body ((:encode-body opts) wal-rows
                                           (batch/context descriptor))]
                           (charge-fn descriptor (max 0 (- (alength body) estimate)))
                           body)))]
        (when defer?
          (if (and wal-body (> (alength ^bytes wal-body) 4096))
            ((:update-tail! opts) rows native-db)
            (apply-rows! rows)))
        (batch/set-data! descriptor
                         {:rows rows :wal-rows wal-rows :wal-body wal-body
                          :result :transacted}))
      1
      (finally (vreset! valid? false)))))

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
                                            [wdb descriptor rows wal-rows nil
                                             (l/request-context wdb)])))
                               :native-prepare-flush! (:preparation-flush! opts)
                               :native-batch-abort! abort!
                               :native-transaction-failed! (fn [t]
                                                             (when-not @failure
                                                               (vreset! failure t)))
                               :native-request-context nil
                               :request-context (batch/context descriptor)))
        _ (when-let [view (:active-view opts)] (vreset! view wdb))]
    (when-let [staged? (:storage-staged? opts)] (vreset! staged? false))
    (try
      (let [result (resolve-member! batch descriptor wdb failure aborted? opts)
            wal-body (when (and encode-body (not (:encode-batch? opts))
                                 (pos? (.size rows)))
                         (let [estimate (long (if body-cost (body-cost rows) 0))]
                           (charge-fn descriptor estimate)
                           (let [^bytes body (encode-body (if (:prepared-request? opts) wal-rows rows)
                                                         (batch/context descriptor))]
                             (charge-fn descriptor (max 0 (- (alength body) estimate)))
                             body)))]
          (batch/set-data! descriptor
                           (if (:encode-batch? opts)
                             {:rows rows :wal-body wal-body :result result
                              :wal-rows wal-rows
                              :storage-staged? (boolean (some-> (:storage-staged? opts) deref))}
                             {:rows rows :wal-body wal-body :result result}))
          (if (and (.isEmpty rows) (not (some-> (:storage-staged? opts) deref))) 0 1))
      (finally (vreset! valid? false)
               (let [_ (with-meta wdb (dissoc (meta wdb) :request-context))] nil)
               (when-let [view (:active-view opts)] (vreset! view nil))))))

(defn- encode-members!
  "Freeze consecutive body writes together, preserving prepared KV regions
  and the final Datalog metadata trailer in exactly their native order."
  [b encode-body defer?]
  (let [term (:ha-term (batch/context (batch/batch-at b 0)))
        encode-body (fn [rows opts] (encode-body rows (assoc opts :ha-term term)))
        parts (ArrayList.) rows (volatile! (RowRegions.))
        flush! (fn []
                 (when-not (.isEmpty ^RowRegions @rows)
                   (.add parts @rows)
                   (vreset! rows (RowRegions.))))]
    (dotimes [idx (batch/batch-count b)]
      (let [d (batch/batch-at b idx)
            data (batch/data d)]
        (when-not (= term (:ha-term (batch/context d)))
          (batch/cancel-before-dispatch!
            (ex-info "Cannot combine writes from different HA terms"
                     {:error :ha/write-rejected :reason :leadership-changed
                      :retryable? true})))
        (when-let [body (:wal-body data)]
          (flush!)
          (.add parts body)
          (batch/set-data! d (batch/with-data-wal-body! data nil)))
        (when-let [wal-rows (:wal-rows data)]
          (.append ^RowRegions @rows wal-rows))))
    ;; The final region needs no replacement accumulator.
    (when-not (.isEmpty ^RowRegions @rows)
      (.add parts @rows))
    (when-not (.isEmpty parts) (wal/prepare-body parts encode-body defer?))))

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
             dispatched? (volatile! false)
             ready? (volatile! false)
             opts (cond-> (assoc opts :active-view active-view :capture-batch batch)
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
                                     (on-error! failure) (throw failure))))))
                           (phase/phase! (if @dispatched?
                                           :native-rows-tail :native-rows-prefix)
                                         {:batch batch :rows (count rows)}))
             tail (volatile! nil)
             storage-target (volatile! nil)
             storage-staged? (volatile! false)
             storage-opts (volatile! nil)
             flush-storage!
             (fn []
               (try
                 (when-let [take-rows! (:storage-rows! opts)]
                   (phase/phase! :storage-encode-start batch)
                   (when-let [txs (take-rows!)]
                     (let [[view descriptor rows wal-rows state context] @storage-target
                           ;; A trusted resolver can stage storage without
                           ;; allocating carriers. Materialize them at the drain,
                           ;; either in its active scope or its retained data.
                           active? (and state (identical? descriptor (aget ^objects state 0)))
                           _ (when active? (member-carriers! state))
                           rows (or rows (when active? (aget ^objects state 1))
                                    (:rows (batch/data descriptor)))
                           wal-rows (or wal-rows (when active? (aget ^objects state 2))
                                        (:wal-rows (batch/data descriptor)))
                           rows (if (instance? RowRegions rows) rows (RowRegions.))
                           wal-rows (if (instance? RowRegions wal-rows) wal-rows (RowRegions.))]
                       (when (and state (not active?))
                         (batch/set-data! descriptor
                                          (batch/with-data-rows! (batch/data descriptor)
                                                             rows wal-rows)))
                       ;; The batch owns these carriers after the request view
                       ;; expires. Never re-enter that request's capture.
                       (l/write-txn view)
                       (let [prepared ((:prepare-rows! opts) raw descriptor nil txs
                                       :data :data)]
                         (capture-prepared! view apply-rows! rows wal-rows
                                            (assoc @storage-opts :capture-context context)
                                            prepared))))
                   (phase/phase! :storage-encode-complete batch))
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
             freeze-storage!
             (fn []
               (let [[view descriptor rows wal-rows _ context] @storage-target]
                 (if (and *defer-storage-encoding?* wal (:shared-datalog-writer? opts)
                          (:storage-tail! opts) (:datalog-prepare? context))
                   (when-let [plan ((:storage-tail! opts))]
                     (let [region (DeferredRows.
                                    (int (:row-count plan))
                                    (reify Supplier
                                      (get [_]
                                        (phase/phase! :storage-encode-start batch)
                                        (let [rows ((:encode plan))]
                                          (phase/phase! :storage-encode-complete batch)
                                          rows))))
                           rows (or rows (:rows (batch/data descriptor)))
                           wal-rows (or wal-rows (:wal-rows (batch/data descriptor)))
                           rows (if (instance? RowRegions rows) rows (RowRegions.))
                           wal-rows (if (instance? RowRegions wal-rows) wal-rows (RowRegions.))]
                       (batch/set-data! descriptor
                                        (batch/with-data-rows!
                                          (batch/data descriptor) rows wal-rows))
                       (capture-prepared!
                         view apply-rows! rows wal-rows
                         (assoc @storage-opts :capture-context context)
                         {:rows region :wal-rows region :storage-tail? true
                          :unconditional-datoms? true
                          :native-tail-bytes (:native-tail-bytes plan)})
                       (phase/phase! :storage-tail-frozen batch)))
                   (flush-storage!))))
             opts (cond-> opts
                    (and wal (:shared-datalog-writer? opts))
                    (assoc :defer-rows!
                           (fn [view prepared context]
                             (let [preparing? (:datalog-prepare? context)
                                   ;; Custom payload/index operations allocate IDs
                                   ;; and resolve values through native reads.
                                   ;; Their raw write scope must stay eager.
                                   eligible? (and preparing? (not l/*raw-kv?*)
                                                  (or (:unconditional-datoms? prepared)
                                                      (not-any?
                                                        #(if (instance? KVTxData %)
                                                           (seq (.-flags ^KVTxData %))
                                                           (.-no-overwrite? ^DatomKVTxData %))
                                                        (:rows prepared))))]
                               (cond
                                 eligible?
                                 (let [rows (or (:rows @tail) (RowRegions.))]
                                   (.append ^RowRegions rows (:rows prepared))
                                   (vreset! tail {:rows rows :view view :prepared? true
                                                  :storage-tail? (or (:storage-tail? @tail)
                                                                     (:storage-tail? prepared))
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
                               (if-let [^objects scope (:native-request-context (meta view))]
                                 (aset scope 4 (dissoc (aget scope 4) :datalog-prepare?))
                                 (let [_ (with-meta view
                                           (update (meta view) :request-context
                                                   dissoc :datalog-prepare?))] nil))))))
             opts (if (:storage-rows! opts)
                    (assoc opts :storage-target storage-target
                           :storage-staged? storage-staged?
                           :storage-flush! flush-storage!) opts)
             ;; Each following member drains this tail before invoking user
             ;; code. Only the final large KV update can survive to dispatch;
             ;; mixed Datalog execution retains its existing visibility rules.
             opts (cond-> opts
                    (and wal (not (:shared-datalog-writer? opts))
                         (not (:finish-preparation! opts))
                         (:prepared-request? opts) (:prepared-owned? opts)
                         (not (:encode-batch? opts)))
                    (assoc :update-tail!
                           (fn [rows view]
                             (vreset! tail {:rows rows :view view
                                            :prepared? true :large? true}))))
             _ (vreset! storage-opts opts)
             member-setup (volatile! nil)
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
                                   (if (and *reuse-datalog-member-setup?*
                                            (:shared-datalog-writer? opts)
                                            (:prepared-request? opts)
                                            (:prepared-owned? opts)
                                            (:encode-batch? opts)
                                            (:datalog-prepare? (batch/context d))
                                            ;; A lone resolver cannot amortize
                                            ;; a shared scope. Queue observation
                                            ;; is only a setup hint, never member
                                            ;; selection or a wait for a caller.
                                            (or @member-setup
                                                (> (batch/batch-count batch) 1)
                                                (some? (.peek ^java.util.Queue
                                                          (.ready ^datalevin.tx_group.batch.Collector
                                                            (batch/batch-collector batch))))))
                                     (let [setup (or @member-setup
                                                     (let [setup (datalog-member-setup
                                                                   raw wdb apply-rows! failure opts)]
                                                       (vreset! member-setup setup)
                                                       setup))]
                                       (run-datalog-member! batch d failure opts setup))
                                     (if (:kv-update? (batch/context d))
                                       (run-update-member! raw wdb apply-rows! batch d failure opts)
                                       (run-member! raw wdb apply-rows! batch d failure opts))))
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
             _ (freeze-storage!)
             extra (when-let [finish! (:finish-preparation! opts)]
                       (finish! wdb batch))
               last-member (when (seq extra)
                             (batch/batch-at batch (dec (batch/batch-count batch))))
               previous (when last-member (batch/data last-member))
               extra (when (seq extra)
                       ((:prepare-rows! opts) raw last-member nil extra :data :data))
               suffix-start (if extra
                              (let [pending @tail]
                                ;; The trailer follows every request, including
                                ;; prepared KV callers sharing this environment.
                                ;; A frozen suffix has no resolver reads. Attach
                                ;; it in order instead of draining the tail just
                                ;; to place the metadata after it.
                                (when (< suffix-start (batch/batch-count batch))
                                  (let [rows (RowRegions.)]
                                    (when pending (.append rows (:rows pending)))
                                    (doseq [idx (range suffix-start (batch/batch-count batch))]
                                      (.append rows (:rows (batch/data (batch/batch-at batch idx)))))
                                    (vreset! tail (assoc (or pending {:view wdb :prepared? true})
                                                       :rows rows))))
                                (long (batch/batch-count batch)))
                              suffix-start)
               weight (if (and extra (zero? (count (:rows previous)))) (inc weight) weight)]
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
                 (.add bodies ((:encode-body opts) (:wal-rows extra)
                               (batch/context last-member))))
               (batch/set-data! last-member
                                (if (:encode-batch? opts)
                                  (let [wal-rows (RowRegions.)]
                                    (when-let [before (:wal-rows previous)]
                                      (.append wal-rows before))
                                    (.append wal-rows (:wal-rows extra))
                                    (batch/with-data-rows! previous rows wal-rows))
                                  (assoc previous :rows rows
                                         :wal-body (codec/combine-commit-row-payloads bodies))))))
         (batch/set-accepted-count!
           batch
           (if (:encode-batch? opts)
             (if-let [body (encode-members!
                            batch (:encode-body opts)
                            (or (:storage-tail? @tail) (> weight 1) (:large? @tail)
                                (and @tail (> (.size ^List (:rows @tail)) 256))))]
               (let [d (batch/batch-at batch (dec (batch/batch-count batch)))]
                 (batch/set-data! d (batch/with-data-wal-body! (batch/data d) body))
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
                    (or (:encode-batch? opts)
                        (not (:collect-prepared-prefix? opts))
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
         (vreset! dispatched? true)
         (phase/phase! :native-dispatch-start batch)
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
