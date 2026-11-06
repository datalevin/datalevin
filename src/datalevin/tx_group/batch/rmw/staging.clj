;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.rmw.staging
  "Storage drains and native tails, owned by one writer's preparation."
  (:require [datalevin.lmdb :as l]
            [datalevin.kv.encoding :as encoding]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.rmw.application :as application]
            [datalevin.tx-group.phase :as phase]
            [datalevin.txlog.codec :as codec])
  (:import [datalevin.lmdb DatomKVTxData KVTxData]
           [datalevin.utl DeferredRows RowRegions WriterContext]
           [java.util ArrayList]
           [java.util.function Supplier]))

(defrecord Staging
           [application wal opts apply-rows! tail storage-target storage-staged? capture-options])

(defn member-carriers!
  "Allocate owned row regions only when this member captures physical rows."
  [^WriterContext state]
  (.ensureRows state)
  state)

(defn capture-prepared!
  "Append storage owned by this native batch, including a late storage drain."
  [wdb apply-rows! rows wal-rows
   {:keys [defer-rows! capture-context capture-batch]} prepared]
  (let [context (or capture-context (l/request-context wdb))
        deferred? (boolean (and defer-rows! (defer-rows! wdb prepared context)))]
    (phase/phase! (if deferred? :native-capture-deferred :native-capture-eager)
                  {:batch capture-batch :rows (count (:rows prepared))
                   :storage-tail? (boolean (:storage-tail? prepared))
                   :preparing? (boolean (:datalog-prepare? context))
                   :raw? (boolean l/*raw-kv?*)})
    (when-not deferred? (apply-rows! (:rows prepared)))
    ;; A rejected fused insert leaves no rows in the writer or its replay log.
    (.append ^RowRegions rows (:rows prepared))
    (.append ^RowRegions wal-rows (:wal-rows prepared))))

(defn flush-storage!
  "Drain staged storage into its owning member, including an expired scope."
  [^Staging staging]
  (let [{:keys [application opts apply-rows! storage-target capture-options]} staging
        {:keys [raw view batch failure]} application
        wdb view]
    (try
      (when-let [take-rows! (:storage-rows! opts)]
        (phase/phase! :storage-encode-start batch)
        (when-let [txs (take-rows! wdb)]
          (let [[view descriptor rows wal-rows state context] @storage-target
                ^WriterContext state state
                ;; A trusted resolver can stage storage without
                ;; allocating carriers. Materialize them at the drain,
                ;; either in its active scope or its retained data.
                active? (and state (identical? descriptor (.descriptor state)))
                _ (when active? (member-carriers! state))
                rows (or rows (when active? (.rows state))
                         (:rows (batch/data descriptor)))
                wal-rows (or wal-rows (when active? (.walRows state))
                             (:wal-rows (batch/data descriptor)))
                rows (if (instance? RowRegions rows) rows (RowRegions.))
                wal-rows (if (instance? RowRegions wal-rows) wal-rows (RowRegions.))]
            (when (and state (not active?))
              (batch/set-data! descriptor
                               (batch/with-data-rows! (batch/data descriptor)
                                 rows wal-rows)))
            ;; The batch owns these carriers after the request view
            ;; expires. Validate its native writer, without borrowing
            ;; the expired request's writer or re-entering its capture.
            (l/write-txn wdb)
            (let [prepared ((:prepare-rows! opts) raw descriptor nil
                                                  (encoding/storage-rows txs)
                                                  :data :data)]
              (capture-prepared! view apply-rows! rows wal-rows
                                 (assoc @capture-options :capture-context context)
                                 prepared))))
        (phase/phase! :storage-encode-complete batch))
      (catch Throwable t
        (let [failure-error
              (try
                (if-let [on-error! (:application-error! opts)]
                  (on-error! t) (throw t))
                (catch Throwable e e))]
          (when-not @failure (vreset! failure failure-error))
          (throw failure-error))))))

(defn flush-tail!
  "Detach a pending native tail before applying it or replaying after growth."
  [^Staging staging]
  (let [{:keys [application apply-rows! tail]} staging
        {:keys [batch failure]} application]
    (when-let [{:keys [rows view]} @tail]
      ;; Detach before applying: a map resize rebuilds
      ;; the writer and must not recursively flush.
      (vreset! tail nil)
      (.setBeforeRead ^WriterContext (l/writer-context view) nil)
      (try (phase/phase! :native-tail-start batch)
           (apply-rows! rows)
           (phase/phase! :native-tail-applied batch)
           (catch Throwable t
             (when-not @failure (vreset! failure t))
             (throw t))))))

(defn freeze-storage!
  "Freeze the final storage region for deferred encoding, or drain it eagerly."
  [^Staging staging]
  (let [{:keys [application wal opts apply-rows! storage-target capture-options]} staging
        {:keys [view batch]} application
        wdb view
        [view descriptor rows wal-rows _ context] @storage-target]
    (if (and (:defer-storage-encoding? opts) wal (:shared-datalog-writer? opts)
             (:storage-tail! opts) (:datalog-prepare? context))
      (when-let [plan ((:storage-tail! opts) wdb)]
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
           (assoc @capture-options :capture-context context)
           {:rows region :wal-rows region :storage-tail? true
            :unconditional-datoms? true
            :native-tail-bytes (:native-tail-bytes plan)})
          (phase/phase! :storage-tail-frozen batch)))
      (flush-storage! staging))))

(defn defer-prepared!
  "Stage eligible Datalog rows; raw and conditional writes retain native visibility."
  [^Staging staging view prepared context]
  (let [{:keys [tail]} staging
        preparing? (:datalog-prepare? context)
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
      (do (flush-tail! staging)
          (when (and (not l/*raw-kv?*)
                     (> (long (:native-tail-bytes prepared 0)) 4096))
            (vreset! tail {:rows (:rows prepared) :view view})
            (.setBeforeRead ^WriterContext (l/writer-context view) #(flush-tail! staging))
            true)))))

(defn member-options [^Staging staging]
  @(:capture-options staging))

(defn pending-tail [^Staging staging]
  @(:tail staging))

(defn reset-member! [^Staging staging]
  (vreset! (:storage-staged? staging) false))

(defn member-staged? [^Staging staging]
  (boolean @(:storage-staged? staging)))

(defn remember-storage!
  "Remember the member and its carriers before its native view expires."
  [^Staging staging view descriptor rows wal-rows scope context]
  (vreset! (:storage-staged? staging) true)
  (vreset! (:storage-target staging) [view descriptor rows wal-rows scope context]))

(defn before-member!
  "User bodies need native state; trusted resolvers may extend a prepared tail."
  [^Staging staging descriptor]
  (let [tail (pending-tail staging)]
    (when-not (and (:datalog-prepare? (batch/context descriptor))
                   (or (nil? tail) (:prepared? tail)))
      (flush-storage! staging)
      (flush-tail! staging))))

(defn create
  "Install callback adapters once. Staging owns their mutable targets and tail;
  resume-member! retires any reusable scope that becomes visible to user code."
  [application wal opts resume-member!]
  (let [apply-rows! #(application/apply-rows! application %)
        staging (->Staging application wal opts apply-rows! (volatile! nil)
                           (volatile! nil) (volatile! false) (volatile! nil))
        opts (cond-> (assoc opts :capture-batch (:batch application))
               (and wal (:shared-datalog-writer? opts))
               (assoc :defer-rows! #(defer-prepared! staging %1 %2 %3))
               (:shared-datalog-writer? opts)
               (assoc :preparation-flush!
                      (fn []
                        (flush-storage! staging)
                        (flush-tail! staging)
                        (when-let [view (application/active-member-view application)]
                          (resume-member! view (member-options staging)))))
               (:storage-rows! opts)
               (assoc :storage-flush! #(flush-storage! staging))
               (and wal (not (:shared-datalog-writer? opts))
                    (not (:finish-preparation! opts))
                    (:prepared-request? opts)
                    (not (:encode-batch? opts)))
               (assoc :update-tail!
                      (fn [rows view]
                        (vreset! (:tail staging)
                                 {:rows rows :view view :prepared? true :large? true}))))]
    (vreset! (:capture-options staging) (assoc opts :application application :staging staging))
    staging))

(defn finish!
  "Freeze final storage and attach metadata after every member's native/WAL rows.
  Return the logical write weight and the first remaining prepared suffix index."
  [^Staging staging weight suffix-start]
  (freeze-storage! staging)
  (let [{:keys [application opts apply-rows! tail]} staging
        {:keys [raw view batch]} application
        weight (long weight)
        suffix-start (long suffix-start)
        wdb view
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
    [weight suffix-start]))
