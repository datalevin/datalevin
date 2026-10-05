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
            [datalevin.txlog :as wal])
  (:import [datalevin.cpp Util$DTLVException]
           [datalevin.lmdb KVTxData]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util ArrayList]))

(defn- expected-native-rejection? [^Throwable t]
  (or (and (instance? Util$DTLVException t)
           (let [message (.getMessage t)]
             (or (.startsWith message "MDB_KEYEXIST")
                 (.startsWith message "MDB_BAD_VALSIZE"))))
      (when-let [cause (ex-cause t)] (expected-native-rejection? cause))))

(defn- application-error! [t]
  (if (or (rmw/clean-body-failure? t) (expected-native-rejection? t))
    (batch/cancel-before-dispatch! t)
    (throw t)))

(def ^:dynamic *enabled?* true)

(defn- public-error [t]
  (if (and (ex-cause t) (= "Batch aborted before WAL append" (ex-message t)))
    (ex-cause t) t))

(defn- submit! [collector request]
  (try (batch/submit! collector request)
       (catch Throwable t (throw (public-error t)))))

(defn- caller-context []
  (let [before cpp/*before-write-commit-fn*
        after kvtx/*after-txlog-append-fn*]
    (when (or before after)
      (cond-> {}
        before (assoc :before-append! (bound-fn [] (before {:operation :close-transact-kv})))
        after (assoc :append-info (volatile! nil)
                     :confirm! (bound-fn [context _]
                                 (when-let [info @(:append-info context)]
                                   (after {:operation :close-transact-kv
                                           :txlog-lsn (:lsn info) :append-res info}))))))))

(defn- prepare [raw name txs kt vt]
  (let [log-rows (ArrayList.)
        rows (public/prepare-rows raw (fn [_]) name txs kt vt false true log-rows)]
    {:rows rows :wal-rows log-rows}))

(defn attach!
  "Install the data collector once on an ordinary embedded WAL handle. Shared
  WAL, HA and Datalog integration keep their existing caller paths until M2/M3."
  [db]
  (let [raw (kv/raw-lmdb db)
        info (i/kv-info raw)
        state (wal/state raw)]
    (when (and *enabled?* state (not (:wal-shared? state))
               (not (:ha-mode @info))
               (not (i/dbi-opts raw c/eav)))
      (locking info
        (when-not (:independent-control @info)
          (let [limits (assoc (charge/resolve-limits
                               {:write-batch-size (max 1 (min (:max-requests charge/default-limits)
                                                              (or (:write-batch-size @info)
                                                                  (wal/group-commit @info))))})
                              ;; Existing public calls have no byte-size or
                              ;; preparation-timeout contract. Bound in-flight
                              ;; requests; retain the legacy caller's input size.
                              :rmw-allowance-bytes charge/request-control-bundle)
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
                            :application-error! application-error!
                            :before-append! (fn [b]
                                              (dotimes [idx (batch/batch-count b)]
                                                (let [d (batch/batch-at b idx)]
                                                  (when (seq (:rows (batch/data d)))
                                                    (when-let [before (:before-append! (batch/context d))]
                                                      (try (before) (catch Throwable t (application-error! t))))))))
                            :prepare-rows! (fn [raw _ name txs kt vt]
                                             (prepare raw name txs kt vt))
                            :encode-body #(wal/prepare-append-body %1 %2)
                            :committed! (fn [b _]
                                          (kvtx/finish-batch-commit! state @metadata)
                                          (dotimes [idx (batch/batch-count b)]
                                            (when-let [info (:append-info (batch/context (batch/batch-at b idx)))]
                                              (vreset! info (:append-res @metadata))))
                                          (vreset! metadata nil))}})
                c (batch/create (fn [b]
                                  ;; Native commit and existing metadata cache
                                  ;; publication share the same writer lock as
                                  ;; standalone admin/manual transactions.
                                  (locking (l/write-txn raw) ((:executor runtime) b)))
                                {:limits limits :preparation-timeout-ms 0
                                 :collection-delay-nanos (kv/write-batch-delay-nanos raw)})
                _ (vreset! collector c)
                _ (batch/initialize-prefix! c (long @(:meta-last-applied-lsn state)))
                check! (fn []
                         (when (.get closing)
                           (throw (ex-info "LMDB handle is closed" {:type :lmdb/closed})))
                         (batch/check-serving! c))
                close! (fn []
                         (when (Thread/holdsLock (l/write-txn raw))
                           (throw (ex-info "Close KV outside its transaction" {})))
                         (.set closing true)
                         ;; Let the current native transaction establish its
                         ;; outcome before rejecting queued requests, as close
                         ;; did before collector integration.
                         (locking (l/write-txn raw) (batch/close! c))
                         (when-not (batch/await-quiescence! c 30000)
                           (throw (ex-info "Native writer did not stop before close"
                                           {:error :txlog/native-close-timeout})))
                         (when-not ((:close! runtime))
                           (throw (ex-info "WAL worker did not stop before close"
                                           {:error :txlog/wal-close-timeout})))
                         (when @(:healthy? (:sync-manager state))
                           (wal/force-through! state (dec (long @(:next-lsn state))) 0))
                         (i/close-kv raw))
                body! (fn [body opts]
                        (check!)
                        (let [result (volatile! nil) completed? (volatile! false)]
                          (try (public/run-body c (bound-fn [tx]
                                                   (vreset! result (body tx))
                                                   (vreset! completed? true)
                                                   @result)
                                                (assoc opts :context (merge (:context opts) (caller-context))))
                               (catch Throwable t
                                 (if (and @completed? (= :txlog/request-aborted (:error (ex-data t))))
                                   @result (throw (public-error t)))))))
                control
                {:embedded? true :collector c :body! body! :close! close! :check! check!
                 :transact!
                 (fn [name txs kt vt]
                   (check!)
                   (if (seq txs)
                     (submit!
                       c {:allowance charge/request-control-bundle
                          :context (caller-context)
                          :prepare
                          (fn [_]
                            (let [{:keys [rows wal-rows]} (prepare raw name txs kt vt)
                                  conditional? (boolean (some #(seq (.-flags ^KVTxData %)) rows))]
                              {:rows rows :state-dependent? conditional?
                               :wal-body (when (seq rows) (wal/prepare-append-body wal-rows {}))
                               :result :transacted}))})
                     :transacted))
                 :clear-dbi! #(public/clear-admin! raw state c
                                                 (get-in runtime [:worker :wake!])
                                                 (assoc (i/env-opts raw)
                                                        :check-retention? false
                                                        :write-metadata! (fn [wdb token]
                                                                           (vreset! metadata (kvtx/write-batch-commit-metadata! wdb state token)))
                                                        :committed! (fn [_]
                                                                      (kvtx/finish-batch-commit! state @metadata)
                                                                      (vreset! metadata nil))) %)
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
    db))
