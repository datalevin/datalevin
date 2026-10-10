;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.embedded
  "Embedded KV integration. Keep the existing opener, catalog, WAL metadata,
  snapshot/recovery layout and administration; replace only data collection."
  (:require [datalevin.binding.cpp.lifecycle :as lifecycle]
            [datalevin.binding.cpp :as cpp]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.kv.snapshot :as snapshot]
            [datalevin.kv.scheduler :as scheduler]
            [datalevin.kv.txlog :as kvtx]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.factory :as factory]
            [datalevin.tx-group.batch.public :as public]
            [datalevin.tx-group.batch.rmw :as rmw]
            [datalevin.txlog :as wal]
            [datalevin.txlog.append :as append])
  (:import [datalevin.cpp Util$DTLVException]
           [datalevin.lmdb DatomKVTxData KVTxData]
           [datalevin.utl RowRegions]
           [org.eclipse.collections.impl.list.mutable FastList]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util ArrayList Arrays]))

(defn- expected-native-rejection? [^Throwable t]
  (or (and (instance? Util$DTLVException t)
           (let [message (.getMessage t)]
             (or (.startsWith message "MDB_KEYEXIST")
                 (.startsWith message "MDB_BAD_VALSIZE"))))
      (when-let [cause (ex-cause t)] (expected-native-rejection? cause))))

(def ^:dynamic *defer-attachment?*
  "Defer attachment while a server re-index replaces its environment."
  false)

(defn- application-error! [t]
  (if (or (rmw/clean-body-failure? t) (expected-native-rejection? t))
    (batch/cancel-before-dispatch! t)
    (throw t)))

(defn- public-error [t]
  (let [cause (ex-cause t)]
    (cond
      (and cause (= "Batch aborted before WAL append" (ex-message t))) cause
      ;; Keep the established HA wire error visible to idempotent callers,
      ;; together with the collector's already committed local WAL outcome.
      (and cause (= :ha/write-indeterminate (:error (ex-data cause))))
      (ex-info (ex-message cause) (merge (ex-data t) (ex-data cause)) cause)
      :else t)))

(defn- submit! [collector request]
  (try (batch/submit! collector request)
       (catch Throwable t (throw (public-error t)))))

(defn- caller-context []
  (let [before cpp/*before-write-commit-fn*
        after kvtx/*after-txlog-append-fn*
        term kvtx/*commit-payload-ha-term*]
    (when (or before after term)
      (cond-> {}
        term (assoc :ha-term term)
        before (assoc :before-append! (bound-fn [] (before {:operation :close-transact-kv})))
        after (assoc :append-info (volatile! nil)
                     :confirm! (bound-fn [context _]
                                 (when-let [info @(:append-info context)]
                                   (after {:operation :close-transact-kv
                                           :txlog-lsn (:lsn info) :append-res info}))))))))

(defn- prepare
  ([raw name txs kt vt] (prepare raw name txs kt vt false))
  ([raw name txs kt vt owned-datoms?] (prepare raw name txs kt vt owned-datoms? nil))
  ([raw name txs kt vt owned-datoms? scratch]
  (let [log-rows (ArrayList.) datom? (l/datom-kv-txs? txs)
        datom-bytes (when datom? (long-array 1))
        rows (if datom?
               (let [datoms (FastList.) other (FastList.)]
                 (doseq [tx txs]
                   (if (instance? DatomKVTxData tx)
                     (let [^DatomKVTxData tx tx
                           ^bytes avg (.-avg tx)
                           ;; Storage's indexable-bytes already detaches AVG
                           ;; from its scratch buffer. Other inputs still copy.
                           frozen (if owned-datoms? tx
                                      (l/->DatomKVTxData
                                        (.-e tx) (Arrays/copyOf avg (alength avg))
                                        (.-added? tx) (.-no-overwrite? tx)))]
                       (.add datoms frozen)
                       (aset datom-bytes 0 (+ (aget datom-bytes 0) 8 (alength avg)))
                       (.add log-rows frozen))
                     (.add other tx)))
                 ;; Storage emits fused datoms before giant/schema/job rows.
                 (let [rows (RowRegions.)]
                   (.append rows datoms)
                   (.append rows (public/prepare-rows raw (fn [_]) name other
                                                     kt vt false true log-rows scratch))
                   rows))
               (public/prepare-rows raw (fn [_]) name txs
                                    kt vt false true log-rows scratch))]
    ;; Only a substantial unconditional datom region can repay the worker
    ;; handoff. Native rejection flags must resolve before WAL dispatch.
    (cond-> {:rows rows :wal-rows log-rows}
      (and datom-bytes (> (aget ^longs datom-bytes 0) 4096)
              (not-any? #(if (instance? KVTxData %)
                           (seq (.-flags ^KVTxData %))
                           (.-no-overwrite? ^DatomKVTxData %)) rows))
      (assoc :native-tail-bytes (aget ^longs datom-bytes 0))))))

(defn attach!
  "Install the data collector once on an existing embedded WAL handle."
  ([db] (attach! db nil))
  ([db hooks]
  (let [raw (kv/raw-lmdb db)
        info (i/kv-info raw)
        state (wal/state raw)]
    (when (and (not *defer-attachment?*) state
               (or (not (:ha-mode @info)) (:server? hooks))
               (or (:datalog? hooks) (not (i/dbi-opts raw c/eav))))
      (locking info
        (when-not (:independent-control @info)
          (let [limits (assoc (charge/resolve-limits
                               {:write-batch-size (max 1 (min (long (:max-requests charge/default-limits))
                                                              (long (or (:write-batch-size @info)
                                                                        (wal/group-commit @info)))))})
                              ;; Existing public calls have no byte-size or
                              ;; preparation-timeout contract. Bound in-flight
                              ;; requests; retain the caller's input size.
                              :rmw-allowance-bytes charge/request-control-bundle)
                ;; Collector execution serializes native owners. Caller-side
                ;; preparation never touches this reusable value buffer.
                scratch (object-array 1)
                collector (volatile! nil)
                closing (AtomicBoolean.)
                metadata (volatile! nil)
                runtime (factory/executor
                          state raw
                          {:runtime-control
                           (merge kvtx/txlog-append-hooks
                           {:check-admission! #(when-not (.get closing)
                                                 (when @collector
                                                   (batch/check-serving! @collector)))
                            :on-failure! #(when @collector (batch/fence! @collector %))})
                           :native-opts
                           {:write-metadata! (fn [wdb token]
                                               (vreset! metadata
                                                        (kvtx/write-batch-commit-metadata!
                                                          wdb state token)))}
                           :rmw-opts
                           {:charge-fn (fn [_ _])
                            :prepared-owned? true :allow-flags? true
                            :prepared-request? true :retry-frozen? true :abort-returns? true
                            :collect-prepared-prefix? true
                            :encode-batch? (:datalog? hooks)
                            :shared-datalog-writer? (:datalog? hooks)
                            :finish-preparation! (:finish-preparation! hooks)
                            :storage-rows! (:storage-rows! hooks)
                            :storage-tail! (:storage-tail! hooks)
                            :before-body! (:before-body! hooks)
                            :ensure-body-ready! (fn [wdb]
                                                  ;; The opener owns flags and scheduler startup.
                                                  ;; Keep recovery as a fallback, but validate the
                                                  ;; payload floor in every acquired writer.
                                                  (when-not (:txlog-recovered? @(i/kv-info wdb))
                                                    (kv/ensure-txlog-ready! wdb false))
                                                  (kvtx/align-runtime-txlog-payload-floor!
                                                    wdb true))
                            :application-error! application-error!
                            :before-append! (fn [b]
                                              (let [term (:ha-term (batch/context (batch/batch-at b 0)))]
                                                (dotimes [idx (batch/batch-count b)]
                                                  (let [d (batch/batch-at b idx)]
                                                    (when-not (= term (:ha-term (batch/context d)))
                                                      (application-error!
                                                        (ex-info "Cannot combine writes from different HA terms"
                                                                 {:error :ha/write-rejected
                                                                  :reason :leadership-changed
                                                                  :retryable? true})))
                                                    (when (or (:storage-staged? (batch/data d))
                                                              (seq (:rows (batch/data d))))
                                                      (when-let [before (:before-append! (batch/context d))]
                                                        (try (before) (catch Throwable t (application-error! t)))))))))
                            :prepare-rows! (fn [raw descriptor name txs kt vt]
                                             (prepare raw name txs kt vt
                                                      (:datalog-prepare? (batch/context descriptor)) scratch))
                            :encode-body #(wal/prepare-append-body %1 %2)
                            :committed! (fn [b _]
                                          (kvtx/finish-batch-commit! state @metadata)
                                          (dotimes [idx (batch/batch-count b)]
                                            (when-let [info (:append-info (batch/context (batch/batch-at b idx)))]
                                              (vreset! info (:append-res @metadata))))
                                          (vreset! metadata nil)
                                          (when-let [committed! (:committed! hooks)]
                                            (committed! b)))}})
                c (batch/create (fn [b]
                                  ;; Native commit and existing metadata cache
                                  ;; publication share the same writer lock as
                                  ;; standalone admin/manual transactions.
                                  (let [execute (fn []
                                                  (locking (l/write-txn raw)
                                                    (if-let [wrap (:wrap-execution hooks)]
                                                      (wrap #((:executor runtime) b) b)
                                                      ((:executor runtime) b))))]
                                    ;; Server admission precedes the native
                                    ;; monitor; explicit remote transactions
                                    ;; acquire their semaphore in that order.
                                    (if-let [wrap (:wrap-batch! hooks)]
                                      (wrap execute b)
                                      (execute))))
                                {:limits limits :preparation-timeout-ms 0
                                 :collection-delay-nanos (kv/write-batch-delay-nanos raw)})
                _ (vreset! collector c)
                _ (batch/initialize-prefix! c (long @(:meta-last-applied-lsn state)))
                check! (fn []
                         (when (.get closing)
                           (throw (ex-info "LMDB handle is closed" {:type :lmdb/closed})))
                         (batch/check-serving! c))
                close! (fn []
                         (when (and (Thread/holdsLock (l/write-txn raw))
                                    (some? @(l/write-txn raw)))
                           (throw (ex-info "Close KV outside its transaction" {})))
                         (.set closing true)
                         ;; Let the current native transaction establish its
                         ;; outcome before rejecting queued requests, as close
                         ;; did before collector integration.
                         (locking (l/write-txn raw) (batch/close! c))
                         ;; An idle writer monitor may be held by store close
                         ;; or administration. Do not wait under that monitor
                         ;; for a collector owner that still needs to acquire it.
                         (when-not (batch/await-quiescence!
                                     c (if (Thread/holdsLock (l/write-txn raw))
                                         0 30000))
                           (throw (ex-info "Native writer did not stop before close"
                                           {:error :txlog/native-close-timeout})))
                         (when-not ((:close! runtime))
                           (throw (ex-info "WAL worker did not stop before close"
                                           {:error :txlog/wal-close-timeout})))
                         (when @(:healthy? (:sync-manager state))
                           (wal/force-through! state (dec (long @(:next-lsn state))) 0))
                         (aset scratch 0 nil)
                         (i/close-kv raw))
                body! (fn [body opts]
                        (check!)
                        (when-let [check-submission! (:check-submission! hooks)]
                          (check-submission!))
                        (let [result (volatile! nil) completed? (volatile! false)
                              submitter (Thread/currentThread)
                              bindings (get-thread-bindings)
                              run (fn [tx]
                                    (vreset! result (body tx))
                                    (vreset! completed? true)
                                    @result)]
                          (try (public/run-body c (fn [tx]
                                                   (if (identical? submitter (Thread/currentThread))
                                                     (run tx)
                                                     (with-bindings bindings (run tx))))
                                                (assoc opts :ready? true
                                                       :context (merge (:context opts) (caller-context))))
                               (catch Throwable t
                                 (if (and @completed? (= :txlog/request-aborted (:error (ex-data t))))
                                   @result (throw (public-error t)))))))
                control
                {:embedded? true :server? (:server? hooks) :datalog-context (:context hooks)
                 :direct-write? (fn [] (and (:server? hooks) kv/*server-write-slot-held?*))
                 :collector c :body! body! :close! close! :check! check!
                 :update!
                 (fn [op]
                   (check!)
                   (when-let [check-submission! (:check-submission! hooks)]
                     (check-submission!))
                   (submit! c {:op op :context (assoc (caller-context) :kv-update? true)}))
                 :internal-body!
                 (fn [op context]
                   (check!)
                   (when-let [check-submission! (:check-submission! hooks)]
                     (check-submission!))
                   (submit! c {:op op :context (merge context (caller-context))}))
                 :transact!
                 (fn [name txs kt vt]
                   (check!)
                   (when-let [check-submission! (:check-submission! hooks)]
                     (check-submission!))
                   (if (seq txs)
                     (let [context (caller-context)]
                       (submit!
                         c {:allowance charge/request-control-bundle
                            :context context
                            :prepare
                            (fn [_]
                              (let [{:keys [rows wal-rows]} (prepare raw name txs kt vt)
                                    conditional? (boolean
                                                   (some #(if (instance? KVTxData %)
                                                            (seq (.-flags ^KVTxData %))
                                                            (.-no-overwrite? ^DatomKVTxData %)) rows))]
                                {:rows rows :state-dependent? conditional?
                                 :wal-body (when (seq rows) (wal/prepare-append-body wal-rows context))
                                 :result :transacted}))}))
                     :transacted))
                 :clear-dbi! #(public/clear-admin! raw state c
                                                 (get-in runtime [:worker :wake!])
                                                 (assoc (i/env-opts raw)
                                                        :check-retention? false
                                                        :wal-context {:ha-term kvtx/*commit-payload-ha-term*}
                                                        :before-append! (fn []
                                                                          (when-let [f cpp/*before-write-commit-fn*]
                                                                            (f {:operation :clear-dbi})))
                                                        :write-metadata! (fn [wdb token]
                                                                           (vreset! metadata (kvtx/write-batch-commit-metadata! wdb state token)))
                                                        :committed! (fn [token]
                                                                      (kvtx/finish-batch-commit! state @metadata)
                                                                      (vreset! metadata nil)
                                                                      (when-let [f kvtx/*after-txlog-append-fn*]
                                                                        (f {:operation :clear-dbi
                                                                            :txlog-lsn (append/last-lsn token)})))) %)
                 :watermarks #(kvtx/txlog-watermarks raw)
                 :force! #(kvtx/with-runtime-txlog-state-guard
                            raw (fn [] (assoc (kvtx/txlog-force-sync! state)
                                              :watermarks (kvtx/txlog-watermarks-map raw state))))
                 :snapshot! #(kvtx/with-write-txn-lock-before-runtime-txlog-state
                               raw (fn [] (kvtx/create-snapshot-now! raw)))
                 :snapshots #(snapshot/list-snapshot-entries raw)
                 :scheduler-state #(scheduler/snapshot-scheduler-state-map raw)
                 :unsupported! (fn [op] (throw (ex-info "Operation requires its own transaction"
                                                       {:operation op :outcome :not-committed})))}]
            (vswap! info assoc :independent-control control
                    :close-independent! close!)
            (lifecycle/register-shutdown-close! raw #(i/close-kv db))))))
    db)))
