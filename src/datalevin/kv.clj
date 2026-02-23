;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.kv
  "KV wrapper type that composes WAL/overlay behavior on top of native cpp LMDB."
  (:refer-clojure :exclude [sync])
  (:require
   [clojure.stacktrace :as stt]
   [datalevin.binding.cpp :as cpp]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [datalevin.interface :as i :refer [ILMDB IList IAdmin]]
   [datalevin.lmdb :as l]
   [datalevin.overlay :as ov]
   [datalevin.scan :as scan]
   [datalevin.spill :as sp]
   [datalevin.util :as u]
   [datalevin.validate :as vld]
   [datalevin.wal :as wal])
  (:import
   [clojure.lang IObj]
   [datalevin.binding.cpp Rtx]
   [datalevin.cpp Txn]
   [datalevin.lmdb KVTxData]
   [java.lang AutoCloseable]
   [java.nio ByteBuffer BufferOverflowException]
   [java.nio.channels FileChannel]
   [java.nio.charset StandardCharsets]
   [java.util Iterator NoSuchElementException Random]
   [java.util.concurrent ConcurrentHashMap]))

(defn- kv-wal-enabled?
  [db]
  (boolean (:kv-wal? @(l/kv-info db))))

(defn- normalize-kv-type
  [t]
  (or t :data))

(defn- encode-kv-bytes
  [x t]
  (let [t (normalize-kv-type t)]
    (loop [^ByteBuffer buf (bf/get-tl-buffer)]
      (let [result (try
                     (b/put-bf buf x t)
                     (b/get-bytes buf)
                     (catch BufferOverflowException _
                       ::overflow))]
        (if (identical? result ::overflow)
          (let [new-buf (ByteBuffer/allocate (* 2 (.capacity buf)))]
            (bf/set-tl-buffer new-buf)
            (recur new-buf))
          result)))))

(defn- committed-overlay
  [db]
  (:kv-overlay-state @(l/kv-info db)))

(defn- private-overlay-state
  [db]
  (:kv-overlay-private-state @(l/kv-info db)))

(defn- ensure-committed-overlay!
  [db]
  (let [info (l/kv-info db)]
    (or (:kv-overlay-state @info)
        (locking info
          (or (:kv-overlay-state @info)
              (let [overlay (assoc (ov/create-overlay db) :private? false)]
                (vswap! info assoc
                        :kv-overlay-state overlay
                        :kv-overlay-env (:env overlay))
                overlay))))))

(defn- ensure-private-overlay!
  [db]
  (let [info (l/kv-info db)]
    (or (:kv-overlay-private-state @info)
        (locking info
          (or (:kv-overlay-private-state @info)
              (let [overlay (assoc (ov/create-overlay db) :private? true)]
                (vswap! info assoc
                        :kv-overlay-private-state overlay
                        :kv-overlay-private-env (:env overlay))
                overlay))))))

(defn- normalize-dbi-opts
  [opts {:keys [key-size val-size]} list?]
  (let [opts  (merge {:key-size key-size
                      :val-size val-size}
                     (or opts {}))
        flags (set (or (:flags opts) c/default-dbi-flags))
        flags (if list? (conj flags :dupsort) flags)]
    (assoc opts
           :flags flags
           :validate-data? (boolean (:validate-data? opts)))))

(defn- wal-open-dbi!
  [db dbi-name opts list?]
  (let [committed (ensure-committed-overlay! db)
        private   (ensure-private-overlay! db)
        existing  (or (ov/overlay-dbi-opts committed dbi-name)
                      (ov/overlay-dbi-opts private dbi-name)
                      (i/dbi-opts (.-native db) dbi-name))
        defaults  (if list?
                    {:key-size c/+max-key-size+ :val-size c/+max-key-size+}
                    {:key-size c/+max-key-size+ :val-size c/*init-val-size*})
        opts*     (normalize-dbi-opts (or opts existing) defaults list?)]
    (when (not= dbi-name c/kv-info)
      (vswap! (l/kv-info db) assoc-in [:dbis dbi-name] opts*))
    (ov/set-dbi-opts! committed dbi-name opts*)
    (ov/set-dbi-opts! private dbi-name opts*)
    (ov/ensure-dbi! private db dbi-name)
    (when (and (nil? existing) (not= dbi-name c/kv-info))
      (.transact-kv db
                    [(l/kv-tx :put c/kv-info [:dbis dbi-name] opts*
                              [:keyword :string])]))
    (ov/ensure-dbi! committed db dbi-name)))

(defn- base-dbi
  [db dbi-name]
  (try
    (datalevin.interface/get-dbi (.-native db) dbi-name false)
    (catch Exception _ nil)))

(defn- ensure-base-dbis-open!
  [db]
  (let [native (.-native db)
        dbis   (:dbis @(l/kv-info db))]
    (when (map? dbis)
      (doseq [[dbi-name opts] dbis]
        (when (and (string? dbi-name)
                   (map? opts)
                   (nil? (try
                           (datalevin.interface/get-dbi native dbi-name false)
                           (catch Exception _ nil))))
          (try
            (binding [c/*bypass-wal* true]
              (datalevin.interface/open-dbi native dbi-name opts))
            (catch Exception _ nil)))))))

(defn- base-iterable
  [dbi rtx iter-fn & args]
  (let [cur (l/get-cursor dbi rtx)
        iterable (apply iter-fn dbi rtx cur args)]
    (reify
      Iterable
      (iterator [_]
        (let [iter (.iterator ^Iterable iterable)]
          (reify
            Iterator
            (hasNext [_] (.hasNext ^Iterator iter))
            (next [_] (.next ^Iterator iter))
            AutoCloseable
            (close [_]
              (when (instance? AutoCloseable iter)
                (.close ^AutoCloseable iter))
              (if (l/read-only? rtx)
                (l/return-cursor dbi cur)
                (l/close-cursor dbi cur)))))))))

(defn- wal-write-txn-ref
  [db]
  (let [info (l/kv-info db)]
    (or (:kv-wal-write-txn @info)
        (locking info
          (or (:kv-wal-write-txn @info)
              (let [v (volatile! nil)]
                (vswap! info assoc :kv-wal-write-txn v)
                v))))))

(defn- wal-write-txn
  [db]
  @(wal-write-txn-ref db))

(defn- wal-writing?
  [db]
  (boolean (and (kv-wal-enabled? db)
                (or (:wal-writing? (meta db))
                    (l/writing? (.-native db)))
                (wal-write-txn db))))

(declare mark-wal-writing
         rebuild-committed-overlay!)

(defn- clear-private-overlay!
  [db]
  (let [info (l/kv-info db)
        overlay (private-overlay-state db)]
    (when overlay
      (ov/reset-deletes! overlay))
    (vswap! info assoc :kv-overlay-private-entries 0)))

(defn- dbi-opts!
  [db dbi-name]
  (if (= dbi-name c/kv-info)
    ;; kv-info DBI is always opened by cpp/open-kv* but not tracked in :dbis.
    {:flags #{} :validate-data? false}
    (or (ov/overlay-dbi-opts (committed-overlay db) dbi-name)
        (ov/overlay-dbi-opts (private-overlay-state db) dbi-name)
        (get-in @(l/kv-info db) [:dbis dbi-name])
        (i/dbi-opts (.-native db) dbi-name)
        (binding [c/*bypass-wal* true]
          (i/get-value (.-native db) c/kv-info
                       [:dbis dbi-name] [:keyword :string] :data))
        (u/raise (str dbi-name " is not open") {}))))

(defn- dbi-dupsort?
  [db dbi-name]
  (boolean (get-in (dbi-opts! db dbi-name) [:flags :dupsort])))

(defn- wal-dbi-open?
  [db dbi-name]
  (or (= dbi-name c/kv-info)
      (ov/overlay-dbi-opts (committed-overlay db) dbi-name)
      (ov/overlay-dbi-opts (private-overlay-state db) dbi-name)
      (get-in @(l/kv-info db) [:dbis dbi-name])
      (base-dbi db dbi-name)))

(defn- ensure-base-dbi-open!
  [db dbi-name]
  (when (and (string? dbi-name)
             (not= dbi-name c/kv-info)
             (nil? (base-dbi db dbi-name)))
    ;; WAL reads may call this from parallel planner threads. Serialize base
    ;; DBI opens to avoid concurrent mdb_dbi_open write txns on the same env.
    (let [info (l/kv-info db)]
      (locking info
        (when (nil? (base-dbi db dbi-name))
          (let [opts   (dbi-opts! db dbi-name)
                native (.-native db)]
            (binding [c/*bypass-wal* true]
              (if (some #{:dupsort} (:flags opts))
                (i/open-list-dbi native dbi-name opts)
                (i/open-dbi native dbi-name opts)))))))))

(defn- ensure-base-dbis-open-for-txs!
  [db dbi-name txs]
  (if dbi-name
    (ensure-base-dbi-open! db dbi-name)
    (doseq [dbi-name* (distinct
                        (keep (fn [tx]
                                (.-dbi-name ^KVTxData (l/->kv-tx-data tx)))
                              txs))]
      (ensure-base-dbi-open! db dbi-name*))))

(defn- validate-kv-txs!
  [db txs dbi-name kt vt]
  (if dbi-name
    (let [opts (dbi-opts! db dbi-name)
          validate? (boolean (:validate-data? opts))
          tx-data (mapv #(l/->kv-tx-data % kt vt) txs)]
      (doseq [tx tx-data]
        (vld/validate-kv-tx-data tx validate?))
      tx-data)
    (let [tx-data (mapv l/->kv-tx-data txs)]
      (doseq [^KVTxData tx tx-data]
        (let [opts (dbi-opts! db (.-dbi-name tx))
              validate? (boolean (:validate-data? opts))]
          (vld/validate-kv-tx-data tx validate?)))
      tx-data)))

(defn- canonical-kv-op
  ([db ^KVTxData tx fallback-dbi]
   (canonical-kv-op db tx fallback-dbi nil))
  ([db ^KVTxData tx fallback-dbi cached-dbi-bytes]
   (let [dbi-name (or (.-dbi-name tx) fallback-dbi)
         op       (.-op tx)
         kt       (normalize-kv-type (.-kt tx))
         vt       (.-vt tx)]
     {:op op
      :dbi dbi-name
      :k (.-k tx)
      :v (.-v tx)
      :kt kt
      :vt vt
      :flags (.-flags tx)
      :dupsort? (dbi-dupsort? db dbi-name)
      :k-bytes (encode-kv-bytes (.-k tx) kt)
      :v-bytes (case op
                 :put (encode-kv-bytes (.-v tx) vt)
                 :put-list (mapv #(encode-kv-bytes % vt) (.-v tx))
                 :del-list (mapv #(encode-kv-bytes % vt) (.-v tx))
                 nil)
      :dbi-bytes (or cached-dbi-bytes
                     (.getBytes ^String dbi-name StandardCharsets/UTF_8))})))

(defn- run-writer-step!
  [step f]
  (vld/check-failpoint step :before)
  (vld/check-failpoint step :during)
  (let [ret (f)]
    (vld/check-failpoint step :after)
    ret))

(defn- recover-committed-overlay-on-writer-error!
  [db]
  ;; A writer may fail after WAL append (step-3+), before overlay publication.
  ;; Rebuild committed overlay from durable WAL tail so reads stay consistent.
  (try
    (wal/refresh-kv-wal-info! db)
    (wal/refresh-kv-wal-meta-info! db)
    (rebuild-committed-overlay! db)
    (catch Exception _
      nil)))

(defn- wal-transact-one-shot!
  [db dbi-name txs k-type v-type]
  (let [info        (l/kv-info db)
        private     (ensure-private-overlay! db)
        private-env (:env private)]
    (try
      (ov/reset-deletes! private)
      (i/open-transact-kv private-env)
      (let [^Rtx priv-rtx @(l/write-txn private-env)
            tx-data (validate-kv-txs! db txs dbi-name k-type v-type)
            dbi-bs (when dbi-name
                     (.getBytes ^String dbi-name StandardCharsets/UTF_8))
            ops (mapv #(canonical-kv-op db % dbi-name dbi-bs) tx-data)
            max-val-op (when (:max-val-size-changed? @info)
                         (let [tx (l/->kv-tx-data
                                   [:put :max-val-size
                                    (:max-val-size @info)]
                                   nil nil)]
                           (vswap! info assoc :max-val-size-changed? false)
                           (canonical-kv-op db tx c/kv-info)))
            ops (if max-val-op (conj ops max-val-op) ops)]
        (when (seq tx-data)
          (doseq [^KVTxData tx tx-data]
            (let [dbi-name* (or (.-dbi-name tx) dbi-name)]
              (ov/ensure-dbi! private db dbi-name*)))
          (i/transact-kv private-env dbi-name txs k-type v-type))
        (when max-val-op
          (i/transact-kv private-env c/kv-info
                         [[:put :max-val-size (:max-val-size @info)]]
                         :data :data))
        (when (seq ops)
          (ov/apply-delete-ops! private ops))
        (let [wal-entry (when (seq ops)
                          (run-writer-step!
                           :step-3
                           #(wal/append-kv-wal-record!
                             db ops (System/currentTimeMillis))))
              wal-id (:wal-id wal-entry)]
          (when wal-id
            (run-writer-step!
             :step-4
             #(let [overlay (ensure-committed-overlay! db)]
                (ov/merge-private->committed! private overlay db priv-rtx)))
            (run-writer-step! :step-5 (fn [] nil))
            (run-writer-step! :step-6 (fn [] nil))
            (run-writer-step! :step-7 (fn [] nil))
            (try
              (run-writer-step!
               :step-8
               #(wal/maybe-publish-kv-wal-meta! db wal-id))
              (catch Exception _ nil))
            ;; Keep default commit path decoupled from replay/resize; only
            ;; pressure-triggered catch-up may flush synchronously.
            (try
              (wal/maybe-flush-kv-indexer-on-pressure! db)
              (catch Exception _ nil))
            ;; Keep user-facing txn latency independent from LMDB replay/resize.
            ;; WAL catch-up happens via background checkpointing or explicit flush.
            nil)
          :transacted))
      (catch Exception e
        (recover-committed-overlay-on-writer-error! db)
        (if-let [edata (ex-data e)]
          (u/raise "Fail to transact to LMDB: " e edata)
          (u/raise "Fail to transact to LMDB: " e {})))
      (finally
        (try
          (i/abort-transact-kv private-env)
          (catch Exception _ nil))
        (try
          (i/close-transact-kv private-env)
          (catch Exception _ nil))
        (ov/reset-deletes! private)
        (vswap! info update :kv-overlay-private-state dissoc :wenv)))))

(defn- wal-transact-open-txn!
  [db dbi-name txs k-type v-type]
  (let [info (l/kv-info db)
        ^Rtx wtxn (wal-write-txn db)]
    (if (nil? wtxn)
      (u/raise "Calling `transact-kv` in WAL mode without opening transaction"
               {})
      (try
        (let [tx-data (validate-kv-txs! db txs dbi-name k-type v-type)
              dbi-bs (when dbi-name
                       (.getBytes ^String dbi-name StandardCharsets/UTF_8))
              ops (mapv #(canonical-kv-op db % dbi-name dbi-bs) tx-data)
              max-val-op (when (:max-val-size-changed? @info)
                           (let [tx (l/->kv-tx-data
                                     [:put :max-val-size
                                      (:max-val-size @info)]
                                     nil nil)]
                             (vswap! info assoc :max-val-size-changed? false)
                             (canonical-kv-op db tx c/kv-info)))
              ops (if max-val-op (conj ops max-val-op) ops)
              private (ensure-private-overlay! db)
              private-env (:env private)]
          (when (seq tx-data)
            (doseq [^KVTxData tx tx-data]
              (let [dbi-name* (or (.-dbi-name tx) dbi-name)]
                (ov/ensure-dbi! private db dbi-name*)))
            (i/transact-kv private-env dbi-name txs k-type v-type))
          (when max-val-op
            (i/transact-kv private-env c/kv-info
                           [[:put :max-val-size (:max-val-size @info)]]
                           :data :data))
          (when (seq ops)
            (ov/apply-delete-ops! private ops)
            (vswap! (.-wal-ops wtxn) into ops))
          (vswap! info assoc
                  :kv-overlay-private-entries (ov/entry-count private))
          :transacted)
        (catch Exception e
          (if-let [edata (ex-data e)]
            (u/raise "Fail to transact to LMDB: " e edata)
            (u/raise "Fail to transact to LMDB: " e {})))))))

(defn- wal-close-transact!
  [db]
  (let [info        (l/kv-info db)
        wtxn-ref    (wal-write-txn-ref db)
        ^Rtx wtxn   @wtxn-ref
        private     (private-overlay-state db)
        private-env (:env private)]
    (if (nil? wtxn)
      (u/raise "Calling `close-transact-kv` without opening" {})
      (let [aborted? @(.-aborted? wtxn)
            ops      (when-not aborted? (seq @(.-wal-ops wtxn)))]
        (try
          (let [wal-entry (when (seq ops)
                            (run-writer-step!
                             :step-3
                             #(wal/append-kv-wal-record!
                               db (vec ops)
                               (System/currentTimeMillis))))
                wal-id (:wal-id wal-entry)]
            (when wal-id
              (locking wtxn-ref
                (run-writer-step!
                 :step-4
                 #(let [overlay (ensure-committed-overlay! db)]
                    (ov/merge-private->committed! private overlay db wtxn)))
                (run-writer-step! :step-5 (fn [] nil))
                (run-writer-step! :step-6 (fn [] nil))
                (run-writer-step! :step-7 (fn [] nil))))
            (try
              (i/abort-transact-kv private-env)
              (catch Exception _ nil))
            (try
              (i/close-transact-kv private-env)
              (catch Exception _ nil))
            (vreset! wtxn-ref nil)
            (vswap! info update :kv-overlay-private-state dissoc :wenv)
            (when (.-wal-ops wtxn)
              (vreset! (.-wal-ops wtxn) []))
            (ov/reset-deletes! private)
            (vswap! info assoc :kv-overlay-private-entries 0)
            (when wal-id
              (try
                (run-writer-step!
                 :step-8
                 #(wal/maybe-publish-kv-wal-meta! db wal-id))
                (catch Exception _ nil))
              ;; Keep default commit path decoupled from replay/resize; only
              ;; pressure-triggered catch-up may flush synchronously.
              (try
                (wal/maybe-flush-kv-indexer-on-pressure! db)
                (catch Exception _ nil))
              ;; Keep user-facing txn latency independent from LMDB replay/resize.
              ;; WAL catch-up happens via background checkpointing or explicit flush.
              nil)
            (if aborted? :aborted :committed))
          (catch Exception e
            (recover-committed-overlay-on-writer-error! db)
            (try
              (i/abort-transact-kv private-env)
              (catch Exception _ nil))
            (try
              (i/close-transact-kv private-env)
              (catch Exception _ nil))
            (vreset! wtxn-ref nil)
            (vswap! info update :kv-overlay-private-state dissoc :wenv)
            (ov/reset-deletes! private)
            (vswap! info assoc :kv-overlay-private-entries 0)
            (if-let [edata (ex-data e)]
              (u/raise "Fail to close read/write transaction in LMDB: "
                       e edata)
              (u/raise "Fail to close read/write transaction in LMDB: "
                       e {}))))))))

(defn- wal-abort-transact!
  [db]
  (let [wtxn-ref (wal-write-txn-ref db)]
    (when-let [^Rtx wtxn @wtxn-ref]
      (vreset! (.-aborted? wtxn) true)
      (when (.-wal-ops wtxn)
        (vreset! (.-wal-ops wtxn) []))
      (clear-private-overlay! db)
      (vswap! (l/kv-info db) update :kv-overlay-private-state dissoc :wenv)
      (vreset! wtxn-ref wtxn)
      nil)))

(defn- wal-close-kv!
  [db]
  (let [info (l/kv-info db)]
    (wal/stop-scheduled-wal-checkpoint info)
    (let [open? (try
                  (not (i/closed-kv? db))
                  (catch Throwable _ false))]
      ;; Sync any unsynced WAL records before close so synced watermark is truthful.
      (let [sync-ok?
            (when-let [^FileChannel ch (:wal-channel @info)]
              (when (.isOpen ch)
                (let [sync-mode (or (:wal-sync-mode @info) c/*wal-sync-mode*)
                      close-mode (if (= sync-mode :none) :fdatasync sync-mode)]
                  (try
                    (wal/sync-channel! ch close-mode)
                    true
                    (catch Exception e
                      (binding [*out* *err*]
                        (println "WARNING: WAL sync failed on close; not promoting synced watermark")
                        (stt/print-stack-trace e))
                      false)))))]
        (when (and (:kv-wal? @info) sync-ok? open?)
          (when-let [committed (:last-committed-wal-tx-id @info)]
            (vswap! info assoc :last-synced-wal-tx-id committed))))
      (when (and (:kv-wal? @info) open?)
        (when-let [wal-id (:last-committed-wal-tx-id @info)]
          (when (pos? ^long wal-id)
            (try
              (wal/publish-kv-wal-meta! db wal-id (System/currentTimeMillis))
              (catch Throwable e
                (binding [*out* *err*]
                  (println "WARNING: Failed to flush WAL meta on close")
                  (stt/print-stack-trace e)))))))
      (wal/close-segment-channel! (:wal-channel @info))
      (when-let [overlay (:kv-overlay-state @info)]
        (ov/close-overlay! overlay)
        (vswap! info dissoc :kv-overlay-state :kv-overlay-env))
      (when-let [overlay (:kv-overlay-private-state @info)]
        (ov/close-overlay! overlay)
        (vswap! info dissoc :kv-overlay-private-state :kv-overlay-private-env))
      (vswap! info dissoc :wal-runtime-ready? :kv-wal-write-txn))))

(defn- rebuild-committed-overlay!
  [db]
  (let [{:keys [last-committed-wal-tx-id last-indexed-wal-tx-id]}
        (wal/kv-wal-watermarks db)
        committed (long (or last-committed-wal-tx-id 0))
        indexed   (long (or last-indexed-wal-tx-id 0))]
    (when (> committed indexed)
      (let [overlay (ensure-committed-overlay! db)
            records (wal/read-wal-records (i/env-dir db) indexed committed)
            ops     (mapcat :wal/ops records)]
        (ov/reset-overlay! overlay)
        (when (seq ops)
          (ov/apply-ops! overlay db ops))))))

(defn- init-kv-wal-runtime!
  [db]
  (let [info (l/kv-info db)]
    (when (:kv-wal? @info)
      (locking info
        (when (and (:kv-wal? @info) (not (:wal-runtime-ready? @info)))
          (wal/refresh-kv-wal-info! db)
          (wal/refresh-kv-wal-meta-info! db)
          (ensure-base-dbis-open! db)
          (ensure-committed-overlay! db)
          (ensure-private-overlay! db)
          (wal-write-txn-ref db)
          (rebuild-committed-overlay! db)
          (wal/start-scheduled-wal-checkpoint info db l/vector-indices)
          (vswap! info assoc :wal-runtime-ready? true))))))

(defn- flush-kv-indexer-if-wal!
  [db]
  (when (and (kv-wal-enabled? db) (not c/*bypass-wal*))
    (i/flush-kv-indexer! db)))

(declare wal-iterate-key
         wal-iterate-key-sample
         wal-iterate-list
         wal-iterate-list-sample
         wal-iterate-list-key-range-val-full
         wal-iterate-list-val-full
         wal-iterate-kv)

(defn with-wal-overlay-scan-bindings
  ([f]
   (letfn [(wal-db? [x]
             (try
               (boolean (:kv-wal? @(l/kv-info x)))
               (catch Throwable _ false)))
           (iter-key [lmdb dbi rtx cur k-range k-type]
             (if (wal-db? lmdb)
               (wal-iterate-key lmdb lmdb dbi rtx cur k-range k-type)
               (l/iterate-key dbi rtx cur k-range k-type)))
           (iter-key-sample [lmdb dbi rtx cur indices budget step
                             k-range k-type]
             (if (wal-db? lmdb)
               (wal-iterate-key-sample lmdb lmdb dbi rtx cur indices budget
                                       step k-range k-type)
               (l/iterate-key-sample dbi rtx cur indices budget step
                                     k-range k-type)))
           (iter-list [lmdb dbi rtx cur k-range k-type v-range v-type]
             (if (wal-db? lmdb)
               (wal-iterate-list lmdb lmdb dbi rtx cur
                                 k-range k-type v-range v-type)
               (l/iterate-list dbi rtx cur k-range k-type v-range v-type)))
           (iter-list-sample [lmdb dbi rtx cur indices budget step
                              k-range k-type]
             (if (wal-db? lmdb)
               (wal-iterate-list-sample lmdb lmdb dbi rtx cur indices budget
                                        step k-range k-type)
               (l/iterate-list-sample dbi rtx cur indices budget step
                                      k-range k-type)))
           (iter-list-key-range-val-full [lmdb dbi rtx cur k-range k-type]
             (if (wal-db? lmdb)
               (wal-iterate-list-key-range-val-full lmdb lmdb dbi rtx cur
                                                    k-range k-type)
               (l/iterate-list-key-range-val-full dbi rtx cur k-range k-type)))
           (iter-list-val-full [lmdb dbi rtx cur]
             (if (wal-db? lmdb)
               (wal-iterate-list-val-full lmdb lmdb dbi rtx cur)
               (l/iterate-list-val-full dbi rtx cur)))
           (iter-kv [lmdb dbi rtx cur k-range k-type v-type]
             (if (wal-db? lmdb)
               (wal-iterate-kv lmdb lmdb dbi rtx cur k-range k-type v-type)
               (l/iterate-kv dbi rtx cur k-range k-type v-type)))]
     (binding [scan/*iterate-key* iter-key
               scan/*iterate-key-sample* iter-key-sample
               scan/*iterate-list* iter-list
               scan/*iterate-list-sample* iter-list-sample
               scan/*iterate-list-key-range-val-full*
               iter-list-key-range-val-full
               scan/*iterate-list-val-full* iter-list-val-full
               scan/*iterate-kv* iter-kv]
       (f))))
  ([_db f]
   (with-wal-overlay-scan-bindings f)))

(defmacro with-wal-overlay-scan
  [db & body]
  `(with-wal-overlay-scan-bindings ~db (fn [] ~@body)))

(defn- wal-get-value
  [db dbi-name k k-type v-type ignore-key?]
  (if (i/list-dbi? db dbi-name)
    (with-wal-overlay-scan db
      (scan/get-first-list-val db dbi-name k k-type v-type ignore-key?))
    (let [k-bytes   (encode-kv-bytes k k-type)
          private   (when (wal-writing? db) (private-overlay-state db))
          committed (committed-overlay db)]
      (if-let [res (and private
                        (ov/get-value private db dbi-name k k-type v-type
                                      ignore-key?))]
        res
        (if (and private (ov/key-deleted? private dbi-name k-bytes))
          nil
          (if-let [res2 (and committed
                             (ov/get-value committed db dbi-name k k-type v-type
                                           ignore-key?))]
            res2
            (if (and committed (ov/key-deleted? committed dbi-name k-bytes))
              nil
              (if (base-dbi db dbi-name)
                (scan/get-value db dbi-name k k-type v-type
                                ignore-key?)
                (if (wal-dbi-open? db dbi-name)
                  nil
                  (u/raise (str dbi-name " is not open") {}))))))))))

(defn- count-iterable
  [^Iterable iterable]
  (with-open [^AutoCloseable iter (.iterator iterable)]
    (loop [n 0]
      (if (.hasNext ^Iterator iter)
        (do
          (.next ^Iterator iter)
          (recur (u/long-inc n)))
        n))))

(defn- wal-get-rank
  [lmdb dbi-name k k-type]
  (let [k-bb (ByteBuffer/wrap (encode-kv-bytes k k-type))]
    (scan/scan
      (with-open [^AutoCloseable iter
                  (.iterator
                    ^Iterable (scan/*iterate-key* lmdb dbi rtx cur
                                                  [:all] k-type))]
        (loop [idx 0]
          (if (.hasNext ^Iterator iter)
            (let [kv (.next ^Iterator iter)]
              (if (zero? (bf/compare-buffer (l/k kv) k-bb))
                idx
                (recur (u/long-inc idx))))
            nil)))
      (u/raise "Fail to get-rank: " e
               {:dbi dbi-name :k k :k-type k-type}))))

(defn- wal-get-by-rank
  [lmdb dbi-name rank k-type v-type ignore-key?]
  (scan/scan
    (with-open [^AutoCloseable iter
                (.iterator
                  ^Iterable (scan/*iterate-kv* lmdb dbi rtx cur
                                               [:all] k-type v-type))]
      (loop [idx 0]
        (if (.hasNext ^Iterator iter)
          (let [kv (.next ^Iterator iter)]
            (if (== idx ^long rank)
              (let [v (when (not= v-type :ignore)
                        (b/read-buffer (l/v kv) v-type))]
                (if ignore-key?
                  (if v v true)
                  [(b/read-buffer (l/k kv) k-type) v]))
              (recur (u/long-inc idx))))
          nil)))
    (u/raise "Fail to get-by-rank: " e
             {:dbi dbi-name :rank rank :k-type k-type :v-type v-type})))

(defn- wal-sample-kv
  [lmdb dbi-name n k-type v-type ignore-key?]
  (let [list? (i/list-dbi? lmdb dbi-name)]
    (scan/scan
      (with-open [^AutoCloseable iter
                  (.iterator
                    ^Iterable (if list?
                                (scan/*iterate-list*
                                  lmdb dbi rtx cur [:all] k-type [:all] v-type)
                                (scan/*iterate-kv* lmdb dbi rtx cur
                                                   [:all] k-type v-type)))]
        (let [holder (sp/new-spillable-vector nil
                                              (:spill-opts (i/env-opts lmdb)))
              rng    (Random.)]
          (loop [idx 0]
            (if (.hasNext ^Iterator iter)
              (let [kv (.next ^Iterator iter)
                    v  (b/read-buffer (l/v kv) v-type)
                    entry (if ignore-key?
                            v
                            [(b/read-buffer (l/k kv) k-type) v])]
                (if (< idx ^long n)
                  (.cons holder entry)
                  (let [j (.nextLong rng (u/long-inc idx))]
                    (when (< j ^long n)
                      (.assocN holder (int j) entry))))
                (recur (u/long-inc idx)))
              (when (>= idx ^long n)
                holder)))))
      (u/raise "Fail to sample-kv: " e
               {:dbi dbi-name :n n :k-type k-type :v-type v-type}))))

(defn- wal-key-range-count
  [lmdb dbi-name k-range k-type]
  (scan/scan
    (count-iterable
      (scan/*iterate-key* lmdb dbi rtx cur k-range k-type))
    (u/raise "Fail to count key range: " e
             {:dbi dbi-name :k-range k-range :k-type k-type})))

(defn- wal-range-count
  [lmdb dbi-name k-range k-type]
  (scan/scan
    (count-iterable
      (scan/*iterate-kv* lmdb dbi rtx cur k-range k-type :data))
    (u/raise "Fail to count range: " e
             {:dbi dbi-name :k-range k-range :k-type k-type})))

(defn- wal-list-range-count
  [lmdb dbi-name k-range k-type _v-range _v-type]
  (scan/scan
    (count-iterable
      ;; Match native approximate semantics: ignore value-range bounds.
      (scan/*iterate-list* lmdb dbi rtx cur k-range k-type [:all] :data))
    (u/raise "Fail to count list range: " e
             {:dbi dbi-name :k-range k-range})))

(defn- wal-key-range-list-count
  [lmdb dbi-name k-range k-type]
  (wal-list-range-count lmdb dbi-name k-range k-type [:all] :data))

(defn- wal-list-count
  [lmdb dbi-name k kt]
  (wal-list-range-count lmdb dbi-name [:closed k k] kt [:all] :data))

(declare wal-iterate-key
         wal-iterate-list
         wal-iterate-list-key-range-val-full
         wal-iterate-list-val-full
         wal-iterate-kv
         wrap-kv wrap-like wrap-dbi)

(deftype KVDBI [db native]
  l/IBuffer
  (put-key [_ x t]
    (l/put-key native x t))
  (put-val [_ x t]
    (l/put-val native x t))

  l/IDB
  (dbi [_]
    (l/dbi native))
  (dbi-name [_]
    (l/dbi-name native))
  (put [_ txn flags]
    (l/put native txn flags))
  (put [_ txn]
    (l/put native txn))
  (del [_ txn all?]
    (l/del native txn all?))
  (del [_ txn]
    (l/del native txn))
  (get-kv [_ rtx]
    (l/get-kv native rtx))
  (get-key-rank [_ rtx]
    (l/get-key-rank native rtx))
  (get-key-by-rank [_ rtx rank]
    (l/get-key-by-rank native rtx rank))
  (iterate-key [_ rtx cur k-range k-type]
    (if (kv-wal-enabled? db)
      (wal-iterate-key db db native rtx cur k-range k-type)
      (l/iterate-key native rtx cur k-range k-type)))
  (iterate-key-sample [_ rtx cur indices budget step k-range k-type]
    (if (kv-wal-enabled? db)
      (wal-iterate-key-sample db db native rtx cur indices budget step
                              k-range k-type)
      (l/iterate-key-sample native rtx cur indices budget step k-range k-type)))
  (iterate-list [_ rtx cur k-range k-type v-range v-type]
    (if (kv-wal-enabled? db)
      (wal-iterate-list db db native rtx cur k-range k-type v-range v-type)
      (l/iterate-list native rtx cur k-range k-type v-range v-type)))
  (iterate-list-sample [_ rtx cur indices budget step k-range k-type]
    (if (kv-wal-enabled? db)
      (wal-iterate-list-sample db db native rtx cur indices budget step
                               k-range k-type)
      (l/iterate-list-sample native rtx cur indices budget step k-range k-type)))
  (iterate-list-key-range-val-full [_ rtx cur k-range k-type]
    (if (kv-wal-enabled? db)
      (wal-iterate-list-key-range-val-full db db native rtx cur k-range k-type)
      (l/iterate-list-key-range-val-full native rtx cur k-range k-type)))
  (iterate-list-val-full [_ rtx cur]
    (if (kv-wal-enabled? db)
      (wal-iterate-list-val-full db db native rtx cur)
      (l/iterate-list-val-full native rtx cur)))
  (iterate-kv [_ rtx cur k-range k-type v-type]
    (if (kv-wal-enabled? db)
      (wal-iterate-kv db db native rtx cur k-range k-type v-type)
      (l/iterate-kv native rtx cur k-range k-type v-type)))
  (get-cursor [_ rtx]
    (l/get-cursor native rtx))
  (cursor-count [_ cur]
    (l/cursor-count native cur))
  (close-cursor [_ cur]
    (l/close-cursor native cur))
  (return-cursor [_ cur]
    (l/return-cursor native cur)))

(defn- wrap-dbi
  [db dbi]
  (cond
    (nil? dbi) nil
    (instance? KVDBI dbi) dbi
    :else (KVDBI. db dbi)))

(defn- native-dbi
  [dbi]
  (if (instance? KVDBI dbi)
    (.-native ^KVDBI dbi)
    dbi))

(defn- dbi-name*
  [dbi]
  (or (some-> dbi native-dbi l/dbi-name)
      scan/*dbi-name*))

(defn- normalize-sample-indices
  ^longs [indices]
  (long-array (sort (map long (or (seq indices) [])))))

(defn- sampled-iterable
  [^Iterable iterable indices]
  (let [^longs idxs (normalize-sample-indices indices)]
    (reify
      Iterable
      (iterator [_]
        (let [iter      (.iterator iterable)
              idx-count (alength idxs)
              closed?   (volatile! false)
              next-kv   (volatile! nil)
              pos       (volatile! -1)
              next-idx  (volatile! 0)
              close!    (fn []
                          (when-not @closed?
                            (vreset! closed? true)
                            (when (instance? AutoCloseable iter)
                              (.close ^AutoCloseable iter))))
              advance!  (fn []
                          (vreset! next-kv nil)
                          (loop []
                            (let [i ^long @next-idx]
                              (if (or (>= i idx-count)
                                      (not (.hasNext ^Iterator iter)))
                                (close!)
                                (let [kv     (.next ^Iterator iter)
                                      p      (long (vswap! pos u/long-inc))
                                      target (aget idxs i)]
                                  (cond
                                    (< p target) (recur)
                                    (= p target) (do
                                                   (vreset! next-kv kv)
                                                   (vswap! next-idx u/long-inc))
                                    :else (do
                                            (vswap! next-idx u/long-inc)
                                            (recur))))))))]
          (advance!)
          (reify
            Iterator
            (hasNext [_]
              (boolean @next-kv))
            (next [_]
              (if-let [kv @next-kv]
                (do
                  (advance!)
                  kv)
                (throw (NoSuchElementException.))))
            AutoCloseable
            (close [_]
              (close!))))))))

(defn- wal-iterate-key
  [db _lmdb dbi rtx cur k-range k-type]
  (let [dbi*       (native-dbi dbi)
        dbi-name   (dbi-name* dbi)
        base-dbi   (when dbi-name (base-dbi db dbi-name))
        base-iter  (cond
                     (and base-dbi (identical? dbi* base-dbi))
                     (l/iterate-key base-dbi rtx cur k-range k-type)

                     base-dbi
                     (base-iterable base-dbi rtx l/iterate-key k-range k-type)

                     :else nil)
        private    (when (wal-writing? db) (private-overlay-state db))
        committed  (committed-overlay db)
        priv-iter  (ov/iter-key private db dbi-name k-range k-type)
        comm-iter  (ov/iter-key committed db dbi-name k-range k-type)
        sources    (cond-> []
                      priv-iter (conj {:iterable priv-iter :priority 0})
                      comm-iter (conj {:iterable comm-iter :priority 1})
                      base-iter (conj {:iterable base-iter :priority 2}))
        priv-del   (ov/delete-view private dbi-name)
        comm-del   (ov/delete-view committed dbi-name)
        delete-views (cond-> []
                        priv-del   (conj (assoc priv-del :priority 0))
                        comm-del   (conj (assoc comm-del :priority 1)))
        forward?   (ov/range-forward? k-range)]
    ;; Native key iterators still expose value buffers to raw visitors.
    (ov/merge-iterables sources delete-views forward? false true)))

(defn- wal-iterate-key-sample
  [db _lmdb dbi rtx cur indices _budget _step k-range k-type]
  (sampled-iterable
    (wal-iterate-key db nil dbi rtx cur k-range k-type)
    indices))

(defn- wal-iterate-list
  [db _lmdb dbi rtx cur k-range k-type v-range v-type]
  (let [dbi*       (native-dbi dbi)
        dbi-name   (dbi-name* dbi)
        base-dbi   (when dbi-name (base-dbi db dbi-name))
        base-iter  (cond
                     (and base-dbi (identical? dbi* base-dbi))
                     (l/iterate-list base-dbi rtx cur k-range k-type v-range v-type)

                     base-dbi
                     (base-iterable base-dbi rtx l/iterate-list
                                    k-range k-type v-range v-type)

                     :else nil)
        private    (when (wal-writing? db) (private-overlay-state db))
        committed  (committed-overlay db)
        priv-iter  (ov/iter-list private db dbi-name k-range k-type v-range v-type)
        comm-iter  (ov/iter-list committed db dbi-name k-range k-type v-range v-type)
        sources    (cond-> []
                      priv-iter (conj {:iterable priv-iter :priority 0})
                      comm-iter (conj {:iterable comm-iter :priority 1})
                      base-iter (conj {:iterable base-iter :priority 2}))
        priv-del   (ov/delete-view private dbi-name)
        comm-del   (ov/delete-view committed dbi-name)
        delete-views (cond-> []
                        priv-del   (conj (assoc priv-del :priority 0))
                        comm-del   (conj (assoc comm-del :priority 1)))
        forward?   (ov/range-forward? k-range)
        v-forward? (ov/range-forward? v-range)]
    (ov/merge-iterables sources delete-views forward? true true v-forward?)))

(defn- wal-iterate-list-sample
  [db _lmdb dbi rtx cur indices _budget _step k-range k-type]
  (sampled-iterable
    (wal-iterate-list db nil dbi rtx cur k-range k-type [:all] :data)
    indices))

(defn- wal-iterate-list-key-range-val-full
  [db _lmdb dbi rtx cur k-range k-type]
  (let [dbi*       (native-dbi dbi)
        dbi-name   (dbi-name* dbi)
        base-dbi   (when dbi-name (base-dbi db dbi-name))
        base-iter  (cond
                     (and base-dbi (identical? dbi* base-dbi))
                     (l/iterate-list-key-range-val-full base-dbi rtx cur
                                                        k-range k-type)

                     base-dbi
                     (base-iterable base-dbi rtx
                                    l/iterate-list-key-range-val-full
                                    k-range k-type)

                     :else nil)
        private    (when (wal-writing? db) (private-overlay-state db))
        committed  (committed-overlay db)
        priv-iter  (ov/iter-list-key-range-val-full private db dbi-name
                                                    k-range k-type)
        comm-iter  (ov/iter-list-key-range-val-full committed db dbi-name
                                                    k-range k-type)
        sources    (cond-> []
                      priv-iter (conj {:iterable priv-iter :priority 0})
                      comm-iter (conj {:iterable comm-iter :priority 1})
                      base-iter (conj {:iterable base-iter :priority 2}))
        priv-del   (ov/delete-view private dbi-name)
        comm-del   (ov/delete-view committed dbi-name)
        delete-views (cond-> []
                        priv-del   (conj (assoc priv-del :priority 0))
                        comm-del   (conj (assoc comm-del :priority 1)))
        ;; Native iterate-list-key-range-val-full ignores `*-back` key direction
        ;; and always yields key/value in forward order.
        forward?   true]
    (ov/merge-iterables sources delete-views forward? true true)))

(defn- wal-iterate-list-val-full
  [db _lmdb dbi rtx cur]
  (let [dbi* (native-dbi dbi)]
    (reify
      l/IListRandKeyValIterable
      (val-iterator [_]
        (let [state (atom nil)]
          (reify
            l/IListRandKeyValIterator
            (seek-key [_ k-value k-type]
              (when-let [it @state]
                (.close ^AutoCloseable it))
              (let [iterable (wal-iterate-list db nil dbi* rtx cur
                                               [:closed k-value k-value] k-type
                                               [:all] :data)
                    it       (.iterator ^Iterable iterable)]
                (reset! state it)
                (.hasNext ^Iterator it)))
            (has-next-val [_]
              (if-let [it @state]
                (.hasNext ^Iterator it)
                false))
            (next-val [_]
              (if-let [it @state]
                (let [kv (.next ^Iterator it)]
                  (l/v kv))
                (throw (NoSuchElementException.))))
            AutoCloseable
            (close [_]
              (when-let [it @state]
                (.close ^AutoCloseable it))
              (reset! state nil))))))))

(defn- wal-iterate-kv
  [db _lmdb dbi rtx cur k-range k-type v-type]
  (let [dbi*       (native-dbi dbi)
        dbi-name   (dbi-name* dbi)
        base-dbi   (when dbi-name (base-dbi db dbi-name))
        base-iter  (cond
                     (and base-dbi (identical? dbi* base-dbi))
                     (l/iterate-kv base-dbi rtx cur k-range k-type v-type)

                     base-dbi
                     (base-iterable base-dbi rtx l/iterate-kv
                                    k-range k-type v-type)

                     :else nil)
        private    (when (wal-writing? db) (private-overlay-state db))
        committed  (committed-overlay db)
        priv-iter  (ov/iter-kv private db dbi-name k-range k-type v-type)
        comm-iter  (ov/iter-kv committed db dbi-name k-range k-type v-type)
        sources    (cond-> []
                      priv-iter (conj {:iterable priv-iter :priority 0})
                      comm-iter (conj {:iterable comm-iter :priority 1})
                      base-iter (conj {:iterable base-iter :priority 2}))
        priv-del   (ov/delete-view private dbi-name)
        comm-del   (ov/delete-view committed dbi-name)
        delete-views (cond-> []
                        priv-del   (conj (assoc priv-del :priority 0))
                        comm-del   (conj (assoc comm-del :priority 1)))
        forward?   (ov/range-forward? k-range)]
    (ov/merge-iterables sources delete-views forward? false true)))

(deftype KV [native ^:unsynchronized-mutable meta]
  l/IWriting
  (writing? [this]
    (if (and (kv-wal-enabled? this) (not c/*bypass-wal*))
      (wal-writing? this)
      (l/writing? native)))
  (write-txn [this]
    (if (and (kv-wal-enabled? this) (not c/*bypass-wal*))
      (wal-write-txn-ref this)
      (l/write-txn native)))
  (kv-info [_] (l/kv-info native))
  (mark-write [this]
    (wrap-like this (l/mark-write native)))
  (reset-write [_]
    (l/reset-write native))

  IObj
  (withMeta [this m]
    (set! meta m)
    this)
  (meta [_] meta)

  ILMDB
  (open-transact-kv [this]
    (if (and (kv-wal-enabled? this) (not c/*bypass-wal*))
      (let [private (ensure-private-overlay! this)
            private-env (:env private)
            wtxn-ref (wal-write-txn-ref this)]
        (locking wtxn-ref
          (when @wtxn-ref
            (u/raise "Calling `open-transact-kv` while transaction is open"
                     {}))
          (ov/reset-deletes! private)
          (let [wenv (i/open-transact-kv private-env)]
            (vreset! wtxn-ref @(l/write-txn private-env))
            (vswap! (l/kv-info this) update :kv-overlay-private-state
                    assoc :wenv wenv)))
        (mark-wal-writing this))
      (wrap-like this (i/open-transact-kv native))))

  (get-value [this dbi-name k]
    (.get-value this dbi-name k :data :data true))
  (get-value [this dbi-name k k-type]
    (.get-value this dbi-name k k-type :data true))
  (get-value [this dbi-name k k-type v-type]
    (.get-value this dbi-name k k-type v-type true))
  (get-value [this dbi-name k k-type v-type ignore-key?]
    (if (kv-wal-enabled? this)
      (wal-get-value this dbi-name k k-type v-type ignore-key?)
      (scan/get-value this dbi-name k k-type v-type ignore-key?)))

  (get-rank [this dbi-name k]
    (.get-rank this dbi-name k :data))
  (get-rank [this dbi-name k k-type]
    (if (kv-wal-enabled? this)
      (with-wal-overlay-scan this
        (wal-get-rank this dbi-name k k-type))
      (scan/get-rank this dbi-name k k-type)))

  (get-by-rank [this dbi-name rank]
    (.get-by-rank this dbi-name rank :data :data true))
  (get-by-rank [this dbi-name rank k-type]
    (.get-by-rank this dbi-name rank k-type :data true))
  (get-by-rank [this dbi-name rank k-type v-type]
    (.get-by-rank this dbi-name rank k-type v-type true))
  (get-by-rank [this dbi-name rank k-type v-type ignore-key?]
    (if (kv-wal-enabled? this)
      (with-wal-overlay-scan this
        (wal-get-by-rank this dbi-name rank k-type v-type ignore-key?))
      (scan/get-by-rank this dbi-name rank k-type v-type ignore-key?)))

  (sample-kv [this dbi-name n]
    (.sample-kv this dbi-name n :data :data true))
  (sample-kv [this dbi-name n k-type]
    (.sample-kv this dbi-name n k-type :data true))
  (sample-kv [this dbi-name n k-type v-type]
    (.sample-kv this dbi-name n k-type v-type true))
  (sample-kv [this dbi-name n k-type v-type ignore-key?]
    (if (kv-wal-enabled? this)
      (with-wal-overlay-scan this
        (wal-sample-kv this dbi-name n k-type v-type ignore-key?))
      (scan/sample-kv this dbi-name n k-type v-type ignore-key?)))

  (key-range-count [this dbi-name k-range]
    (.key-range-count this dbi-name k-range :data))
  (key-range-count [this dbi-name k-range k-type]
    (if (kv-wal-enabled? this)
      (with-wal-overlay-scan this
        (wal-key-range-count this dbi-name k-range k-type))
      (i/key-range-count native dbi-name k-range k-type)))

  (key-range-list-count [this dbi-name k-range k-type]
    (if (kv-wal-enabled? this)
      (with-wal-overlay-scan this
        (wal-key-range-list-count this dbi-name k-range k-type))
      (i/key-range-list-count native dbi-name k-range k-type)))

  (visit-key-sample [this dbi-name indices budget step visitor k-range k-type]
    (.visit-key-sample this dbi-name indices budget step visitor k-range k-type
                       true))
  (visit-key-sample
    [this dbi-name indices budget step visitor k-range k-type raw-pred?]
    (if (kv-wal-enabled? this)
      (with-wal-overlay-scan this
        (scan/visit-key-sample this dbi-name indices budget step visitor
                               k-range k-type raw-pred?))
      (i/visit-key-sample native dbi-name indices budget step visitor
                          k-range k-type raw-pred?)))

  (range-count [this dbi-name k-range]
    (.range-count this dbi-name k-range :data))
  (range-count [this dbi-name k-range k-type]
    (if (kv-wal-enabled? this)
      (with-wal-overlay-scan this
        (wal-range-count this dbi-name k-range k-type))
      (i/range-count native dbi-name k-range k-type)))

  (transact-kv [this txs]
    (.transact-kv this nil txs))
  (transact-kv [this dbi-name txs]
    (.transact-kv this dbi-name txs :data :data))
  (transact-kv [this dbi-name txs k-type]
    (.transact-kv this dbi-name txs k-type :data))
  (transact-kv [this dbi-name txs k-type v-type]
    (locking (l/write-txn this)
      (let [one-shot?    (nil? @(l/write-txn this))
            wal-enabled? (and (kv-wal-enabled? this)
                              (not c/*bypass-wal*))]
        (cond
          (not wal-enabled?)
          (do
            (when (and (kv-wal-enabled? this) c/*bypass-wal*)
              (ensure-base-dbis-open-for-txs! this dbi-name txs))
            (i/transact-kv native dbi-name txs k-type v-type))

          one-shot?
          (wal-transact-one-shot! this dbi-name txs k-type v-type)

          :else
          (wal-transact-open-txn! this dbi-name txs k-type v-type)))))

  (kv-wal-watermarks [this]
    (wal/kv-wal-watermarks this))

  (kv-wal-metrics [this]
    (wal/kv-wal-metrics this))

  (flush-kv-indexer! [this]
    (.flush-kv-indexer! this nil))
  (flush-kv-indexer! [this upto-wal-id]
    (when-let [ch (:wal-channel @(l/kv-info this))]
      (when (.isOpen ^java.nio.channels.FileChannel ch)
        (try (.force ^java.nio.channels.FileChannel ch true)
             (catch Exception _ nil))))
    (let [info         (l/kv-info this)
          prev-indexed (long (or (:last-indexed-wal-tx-id @info) 0))
          res          (wal/flush-kv-indexer! this upto-wal-id)
          indexed      (long (or (:indexed-wal-tx-id res) 0))]
      (locking (l/write-txn this)
        (when (and (> indexed prev-indexed)
                   (kv-wal-enabled? this))
          (when-let [overlay (committed-overlay this)]
            (let [ops (mapcat :wal/ops
                              (wal/read-wal-records
                                (i/env-dir this)
                                prev-indexed
                                indexed))]
              (when (seq ops)
                (ov/prune-committed-overlay! overlay this ops)))))
        (vswap! (l/kv-info this) assoc
                :last-indexed-wal-tx-id (:indexed-wal-tx-id res)
                :applied-wal-tx-id (binding [c/*bypass-wal* true]
                                     (or (i/get-value this c/kv-info
                                                      c/applied-wal-tx-id
                                                      :data :data)
                                         0))))
      res))

  (open-tx-log [this from-wal-id]
    (wal/open-tx-log this from-wal-id))
  (open-tx-log [this from-wal-id upto-wal-id]
    (wal/open-tx-log this from-wal-id upto-wal-id))

  (gc-wal-segments! [this]
    (wal/gc-wal-segments! this))
  (gc-wal-segments! [this retain-wal-id]
    (wal/gc-wal-segments! this retain-wal-id))

  (abort-transact-kv [db]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (wal-abort-transact! db)
      (datalevin.interface/abort-transact-kv native)))
  (check-ready [db] (datalevin.interface/check-ready native))
  (clear-dbi
    [db dbi-name]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (do
        (flush-kv-indexer-if-wal! db)
        (datalevin.interface/clear-dbi native dbi-name)
        (when-let [overlay (committed-overlay db)]
          (ov/clear-dbi! overlay dbi-name))
        (when-let [overlay (private-overlay-state db)]
          (ov/clear-dbi! overlay dbi-name))
        nil)
      (datalevin.interface/clear-dbi native dbi-name)))
  (close-kv [db]
    (if (kv-wal-enabled? db)
      (do
        (wal-close-kv! db)
        (datalevin.interface/close-kv native))
      (datalevin.interface/close-kv native)))
  (close-transact-kv [db]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (wal-close-transact! db)
      (datalevin.interface/close-transact-kv native)))
  (closed-kv? [db] (datalevin.interface/closed-kv? native))
  (copy
    [db dest]
    (flush-kv-indexer-if-wal! db)
    (datalevin.interface/copy native dest))
  (copy
    [db dest compact?]
    (flush-kv-indexer-if-wal! db)
    (datalevin.interface/copy native dest compact?))
  (dbi-opts
    [db dbi-name]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (or (ov/overlay-dbi-opts (committed-overlay db) dbi-name)
          (ov/overlay-dbi-opts (private-overlay-state db) dbi-name)
          (get-in @(l/kv-info db) [:dbis dbi-name])
          (datalevin.interface/dbi-opts native dbi-name)
          (binding [c/*bypass-wal* true]
            (datalevin.interface/get-value native c/kv-info
                                           [:dbis dbi-name]
                                           [:keyword :string]
                                           :data)))
      (datalevin.interface/dbi-opts native dbi-name)))
  (drop-dbi
    [db dbi-name]
    (flush-kv-indexer-if-wal! db)
    (let [existing (or (datalevin.interface/dbi-opts native dbi-name)
                       (get-in @(l/kv-info db) [:dbis dbi-name]))
          ret      (datalevin.interface/drop-dbi native dbi-name)]
      (when (and existing (not= dbi-name c/kv-info))
        (.transact-kv db c/kv-info
                      [[:del [:dbis dbi-name]]]
                      [:keyword :string]))
      (when (and (kv-wal-enabled? db)
                 (not= dbi-name c/kv-info))
        (when-let [overlay (committed-overlay db)]
          (ov/drop-dbi! overlay dbi-name))
        (when-let [overlay (private-overlay-state db)]
          (ov/drop-dbi! overlay dbi-name))
        (vswap! (l/kv-info db) update :dbis dissoc dbi-name))
      ret))
  (entries
    [db dbi-name]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (with-wal-overlay-scan db
        (let [lmdb db]
          (scan/scan
            (count-iterable
              (if (i/list-dbi? db dbi-name)
                (scan/*iterate-list* db dbi rtx cur [:all] :raw [:all] :raw)
                (scan/*iterate-kv* db dbi rtx cur [:all] :raw :raw)))
            (u/raise "Fail to count entries: " e {:dbi dbi-name}))))
      (datalevin.interface/entries native dbi-name)))
  (env-dir [db] (datalevin.interface/env-dir native))
  (env-opts [db] (datalevin.interface/env-opts native))
  (get-dbi
    [db dbi-name]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (do
        (when (wal-dbi-open? db dbi-name)
          (ensure-base-dbi-open! db dbi-name))
        (wrap-dbi db (datalevin.interface/get-dbi native dbi-name false)))
      (wrap-dbi db (datalevin.interface/get-dbi native dbi-name))))
  (get-dbi
    [db dbi-name create?]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (do
        (when (or create? (wal-dbi-open? db dbi-name))
          (ensure-base-dbi-open! db dbi-name))
        (wrap-dbi db
                  (datalevin.interface/get-dbi native dbi-name create?)))
      (wrap-dbi db
                (datalevin.interface/get-dbi native dbi-name create?))))
  (get-env-flags [db] (datalevin.interface/get-env-flags native))
  (get-first
    [db dbi-name k-range]
    (.get-first db dbi-name k-range :data :data false))
  (get-first
    [db dbi-name k-range k-type]
    (.get-first db dbi-name k-range k-type :data false))
  (get-first
    [db dbi-name k-range k-type v-type]
    (.get-first db dbi-name k-range k-type v-type false))
  (get-first
    [db dbi-name k-range k-type v-type ignore-key?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/get-first db dbi-name k-range k-type v-type ignore-key?))
      (datalevin.interface/get-first native dbi-name k-range k-type
                                     v-type ignore-key?)))
  (get-first-n
    [db dbi-name n k-range]
    (.get-first-n db dbi-name n k-range :data :data false))
  (get-first-n
    [db dbi-name n k-range k-type]
    (.get-first-n db dbi-name n k-range k-type :data false))
  (get-first-n
    [db dbi-name n k-range k-type v-type]
    (.get-first-n db dbi-name n k-range k-type v-type false))
  (get-first-n
    [db dbi-name n k-range k-type v-type ignore-key?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/get-first-n db dbi-name n k-range k-type v-type ignore-key?))
      (datalevin.interface/get-first-n native dbi-name n k-range
                                       k-type v-type ignore-key?)))
  (get-range
    [db dbi-name k-range]
    (.get-range db dbi-name k-range :data :data false))
  (get-range
    [db dbi-name k-range k-type]
    (.get-range db dbi-name k-range k-type :data false))
  (get-range
    [db dbi-name k-range k-type v-type]
    (.get-range db dbi-name k-range k-type v-type false))
  (get-range
    [db dbi-name k-range k-type v-type ignore-key?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/get-range db dbi-name k-range k-type v-type ignore-key?))
      (datalevin.interface/get-range native dbi-name k-range
                                     k-type v-type ignore-key?)))
  (get-rtx [db] (datalevin.interface/get-rtx native))
  (get-some
    [db dbi-name pred k-range]
    (.get-some db dbi-name pred k-range :data :data false true))
  (get-some
    [db dbi-name pred k-range k-type]
    (.get-some db dbi-name pred k-range k-type :data false true))
  (get-some
    [db dbi-name pred k-range k-type v-type]
    (.get-some db dbi-name pred k-range k-type v-type false true))
  (get-some
    [db dbi-name pred k-range k-type v-type ignore-key?]
    (.get-some db dbi-name pred k-range k-type v-type ignore-key? true))
  (get-some
    [db dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/get-some db dbi-name pred k-range k-type v-type
                       ignore-key? raw-pred?))
      (datalevin.interface/get-some native dbi-name pred k-range k-type
                                    v-type ignore-key? raw-pred?)))
  (key-compressor [db] (datalevin.interface/key-compressor native))
  (key-range
    [db dbi-name k-range]
    (.key-range db dbi-name k-range :data))
  (key-range
    [db dbi-name k-range k-type]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/key-range db dbi-name k-range k-type))
      (datalevin.interface/key-range native dbi-name k-range k-type)))
  (list-dbis
    [db]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (let [dbis (:dbis @(l/kv-info db))]
        (->> (if (map? dbis)
               (keys dbis)
               (datalevin.interface/list-dbis native))
             (remove #(= % c/kv-info))
             sort
             vec))
      (datalevin.interface/list-dbis native)))
  (max-val-size [db] (datalevin.interface/max-val-size native))
  (open-dbi [db dbi-name] (.open-dbi db dbi-name nil))
  (open-dbi
    [db dbi-name opts]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (locking (l/write-txn db)
        (wrap-dbi db (wal-open-dbi! db dbi-name opts false)))
      (let [existing (datalevin.interface/dbi-opts native dbi-name)
            ret      (wrap-dbi db
                               (datalevin.interface/open-dbi native dbi-name
                                                             opts))]
        (when (and (nil? existing) (not= dbi-name c/kv-info))
          (let [dbi-opts (datalevin.interface/dbi-opts native dbi-name)]
            (binding [c/*bypass-wal* true]
              (.transact-kv db
                            [(l/kv-tx :put c/kv-info [:dbis dbi-name] dbi-opts
                                      [:keyword :string])]))))
        ret)))
  (open-list-dbi
    [db list-name]
    (.open-list-dbi db list-name nil))
  (open-list-dbi
    [db list-name opts]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (locking (l/write-txn db)
        (wrap-dbi db (wal-open-dbi! db list-name opts true)))
      (let [existing (datalevin.interface/dbi-opts native list-name)
            ret      (wrap-dbi db
                               (datalevin.interface/open-list-dbi native list-name
                                                                  opts))]
        (when (and (nil? existing) (not= list-name c/kv-info))
          (let [dbi-opts (datalevin.interface/dbi-opts native list-name)]
            (binding [c/*bypass-wal* true]
              (.transact-kv db
                            [(l/kv-tx :put c/kv-info [:dbis list-name] dbi-opts
                                      [:keyword :string])]))))
        ret)))
  (range-filter
    [db dbi-name pred k-range]
    (.range-filter db dbi-name pred k-range :data :data false true))
  (range-filter
    [db dbi-name pred k-range k-type]
    (.range-filter db dbi-name pred k-range k-type :data false true))
  (range-filter
    [db dbi-name pred k-range k-type v-type]
    (.range-filter db dbi-name pred k-range k-type v-type false true))
  (range-filter
    [db dbi-name pred k-range k-type v-type ignore-key?]
    (.range-filter db dbi-name pred k-range k-type v-type ignore-key? true))
  (range-filter
    [db dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/range-filter db dbi-name pred k-range k-type v-type
                           ignore-key? raw-pred?))
      (datalevin.interface/range-filter native dbi-name pred k-range
                                        k-type v-type ignore-key?
                                        raw-pred?)))
  (range-filter-count
    [db dbi-name pred k-range]
    (.range-filter-count db dbi-name pred k-range :data :data true))
  (range-filter-count
    [db dbi-name pred k-range k-type]
    (.range-filter-count db dbi-name pred k-range k-type :data true))
  (range-filter-count
    [db dbi-name pred k-range k-type v-type]
    (.range-filter-count db dbi-name pred k-range k-type v-type true))
  (range-filter-count
    [db dbi-name pred k-range k-type v-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/range-filter-count db dbi-name pred k-range k-type v-type
                                 raw-pred?))
      (datalevin.interface/range-filter-count native dbi-name pred k-range
                                              k-type v-type raw-pred?)))
  (range-keep
    [db dbi-name pred k-range]
    (.range-keep db dbi-name pred k-range :data :data true))
  (range-keep
    [db dbi-name pred k-range k-type]
    (.range-keep db dbi-name pred k-range k-type :data true))
  (range-keep
    [db dbi-name pred k-range k-type v-type]
    (.range-keep db dbi-name pred k-range k-type v-type true))
  (range-keep
    [db dbi-name pred k-range k-type v-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/range-keep db dbi-name pred k-range k-type v-type raw-pred?))
      (datalevin.interface/range-keep native dbi-name pred k-range
                                      k-type v-type raw-pred?)))
  (range-seq
    [db dbi-name k-range]
    (.range-seq db dbi-name k-range :data :data false nil))
  (range-seq
    [db dbi-name k-range k-type]
    (.range-seq db dbi-name k-range k-type :data false nil))
  (range-seq
    [db dbi-name k-range k-type v-type]
    (.range-seq db dbi-name k-range k-type v-type false nil))
  (range-seq
    [db dbi-name k-range k-type v-type ignore-key?]
    (.range-seq db dbi-name k-range k-type v-type ignore-key? nil))
  (range-seq
    [db dbi-name k-range k-type v-type ignore-key? opts]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/range-seq db dbi-name k-range k-type v-type ignore-key? opts))
      (datalevin.interface/range-seq native dbi-name k-range
                                     k-type v-type ignore-key? opts)))
  (range-some
    [db dbi-name pred k-range]
    (.range-some db dbi-name pred k-range :data :data true))
  (range-some
    [db dbi-name pred k-range k-type]
    (.range-some db dbi-name pred k-range k-type :data true))
  (range-some
    [db dbi-name pred k-range k-type v-type]
    (.range-some db dbi-name pred k-range k-type v-type true))
  (range-some
    [db dbi-name pred k-range k-type v-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/range-some db dbi-name pred k-range k-type v-type raw-pred?))
      (datalevin.interface/range-some native dbi-name pred k-range
                                      k-type v-type raw-pred?)))
  (return-rtx [db rtx] (datalevin.interface/return-rtx native rtx))
  (set-env-flags
    [db ks on-off]
    (datalevin.interface/set-env-flags native ks on-off))
  (set-key-compressor
    [db c]
    (datalevin.interface/set-key-compressor native c))
  (set-max-val-size
    [db size]
    (datalevin.interface/set-max-val-size native size))
  (set-val-compressor
    [db c]
    (datalevin.interface/set-val-compressor native c))
  (stat
    [db]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (let [base (datalevin.interface/stat native)
            n    (count (i/list-dbis db))]
        ;; Native no-arg stat reports metadata DB entries. Preserve that shape
        ;; in WAL mode where user DBIs can be overlay-only.
        (assoc base :entries (long (inc n))))
      (datalevin.interface/stat native)))
  (stat
    [db dbi-name]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (if dbi-name
        (let [base (try
                     (datalevin.interface/stat native dbi-name)
                     (catch Exception _
                       (datalevin.interface/stat native)))]
          (assoc base :entries (long (i/entries db dbi-name))))
        (let [base (datalevin.interface/stat native)
              n    (count (i/list-dbis db))]
          (assoc base :entries (long (inc n)))))
      (datalevin.interface/stat native dbi-name)))
  (sync
    [db]
    (flush-kv-indexer-if-wal! db)
    (datalevin.interface/sync native))
  (sync
    [db force]
    (flush-kv-indexer-if-wal! db)
    (datalevin.interface/sync native force))
  (val-compressor [db] (datalevin.interface/val-compressor native))
  (visit
    [db dbi-name visitor k-range]
    (.visit db dbi-name visitor k-range :data :data true))
  (visit
    [db dbi-name visitor k-range k-type]
    (.visit db dbi-name visitor k-range k-type :data true))
  (visit
    [db dbi-name visitor k-range k-type v-type]
    (.visit db dbi-name visitor k-range k-type v-type true))
  (visit
    [db dbi-name visitor k-range k-type v-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/visit db dbi-name visitor k-range k-type v-type raw-pred?))
      (datalevin.interface/visit native dbi-name visitor k-range
                                 k-type v-type raw-pred?)))
  (visit-key-range
    [db dbi-name visitor k-range]
    (.visit-key-range db dbi-name visitor k-range :data true))
  (visit-key-range
    [db dbi-name visitor k-range k-type]
    (.visit-key-range db dbi-name visitor k-range k-type true))
  (visit-key-range
    [db dbi-name visitor k-range k-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/visit-key-range db dbi-name visitor k-range k-type raw-pred?))
      (datalevin.interface/visit-key-range native dbi-name visitor k-range
                                           k-type raw-pred?)))

  IList
  (list-count [this dbi-name k kt]
    (if (kv-wal-enabled? this)
      (with-wal-overlay-scan this
        (wal-list-count this dbi-name k kt))
      (i/list-count native dbi-name k kt)))

  (near-list [this dbi-name k v kt vt]
    (if (kv-wal-enabled? this)
      (when (and k v)
        (with-wal-overlay-scan this
          (scan/list-range-first-raw-v
            this dbi-name [:closed k k] kt [:at-least v] vt)))
      (i/near-list native dbi-name k v kt vt)))

  (in-list? [this dbi-name k v kt vt]
    (if (kv-wal-enabled? this)
      (if (and k v)
        (boolean (seq (i/list-range this dbi-name
                                    [:closed k k] kt
                                    [:closed v v] vt)))
        false)
      (i/in-list? native dbi-name k v kt vt)))

  (list-range-count [this dbi-name k-range k-type v-range v-type]
    (if (kv-wal-enabled? this)
      (with-wal-overlay-scan this
        (wal-list-range-count this dbi-name k-range k-type v-range v-type))
      (i/list-range-count native dbi-name k-range k-type v-range v-type)))

  (visit-list-sample
    [this dbi-name indices budget step visitor k-range k-type v-type]
    (.visit-list-sample this dbi-name indices budget step visitor
                        k-range k-type v-type true))
  (visit-list-sample
    [this dbi-name indices budget step visitor k-range k-type v-type
     raw-pred?]
    (if (kv-wal-enabled? this)
      (with-wal-overlay-scan this
        (scan/visit-list-sample this dbi-name indices budget step visitor
                                k-range k-type v-type raw-pred?))
      (i/visit-list-sample native dbi-name indices budget step visitor
                           k-range k-type v-type raw-pred?)))
  (del-list-items
    [db list-name k k-type]
    (.transact-kv db [(l/kv-tx :del list-name k k-type)]))
  (del-list-items
    [db list-name k vs k-type v-type]
    (.transact-kv db [(l/kv-tx :del-list list-name k vs k-type v-type)]))
  (get-list
    [db list-name k k-type v-type]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/get-list db list-name k k-type v-type))
      (datalevin.interface/get-list native list-name k k-type v-type)))
  (list-dbi?
    [db dbi-name]
    (if (and (kv-wal-enabled? db) (not c/*bypass-wal*))
      (let [opts (or (i/dbi-opts native dbi-name)
                     (ov/overlay-dbi-opts (committed-overlay db) dbi-name)
                     (ov/overlay-dbi-opts (private-overlay-state db) dbi-name))]
        (boolean (some #{:dupsort} (:flags opts))))
      (datalevin.interface/list-dbi? native dbi-name)))
  (list-range
    [db list-name k-range k-type v-range v-type]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/list-range db list-name k-range k-type v-range v-type))
      (datalevin.interface/list-range native list-name k-range
                                      k-type v-range v-type)))
  (list-range-filter
    [db list-name pred k-range k-type v-range v-type]
    (.list-range-filter db list-name pred k-range k-type v-range v-type true))
  (list-range-filter
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/list-range-filter db list-name pred k-range
                                k-type v-range v-type raw-pred?))
      (datalevin.interface/list-range-filter native list-name pred k-range
                                             k-type v-range v-type raw-pred?)))
  (list-range-filter-count
    [db list-name pred k-range k-type v-range v-type]
    (.list-range-filter-count db list-name pred k-range k-type
                              v-range v-type true))
  (list-range-filter-count
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/list-range-filter-count db list-name pred k-range k-type
                                      v-range v-type raw-pred?))
      (datalevin.interface/list-range-filter-count native list-name pred k-range
                                                   k-type v-range v-type raw-pred?)))
  (list-range-first
    [db list-name k-range k-type v-range v-type]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/list-range-first db list-name k-range k-type v-range v-type))
      (datalevin.interface/list-range-first native list-name k-range
                                            k-type v-range v-type)))
  (list-range-first-n
    [db list-name n k-range k-type v-range v-type]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/list-range-first-n db list-name n k-range k-type v-range v-type))
      (datalevin.interface/list-range-first-n native list-name n k-range
                                              k-type v-range v-type)))
  (list-range-keep
    [db list-name pred k-range k-type v-range v-type]
    (.list-range-keep db list-name pred k-range k-type v-range v-type true))
  (list-range-keep
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/list-range-keep db list-name pred k-range
                              k-type v-range v-type raw-pred?))
      (datalevin.interface/list-range-keep native list-name pred k-range
                                           k-type v-range v-type raw-pred?)))
  (list-range-some
    [db list-name pred k-range k-type v-range v-type]
    (.list-range-some db list-name pred k-range k-type v-range v-type true))
  (list-range-some
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/list-range-some db list-name pred k-range
                              k-type v-range v-type raw-pred?))
      (datalevin.interface/list-range-some native list-name pred k-range
                                           k-type v-range v-type raw-pred?)))
  (put-list-items
    [db list-name k vs k-type v-type]
    (.transact-kv db [(l/kv-tx :put-list list-name k vs k-type v-type)]))
  (visit-list
    [db list-name visitor k k-type]
    (.visit-list db list-name visitor k k-type :data true))
  (visit-list
    [db list-name visitor k k-type v-type]
    (.visit-list db list-name visitor k k-type v-type true))
  (visit-list
    [db list-name visitor k k-type v-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/visit-list db list-name visitor k k-type v-type raw-pred?))
      (datalevin.interface/visit-list native list-name visitor k k-type
                                      v-type raw-pred?)))
  (visit-list-key-range
    [db list-name visitor k-range k-type v-type]
    (.visit-list-key-range db list-name visitor k-range k-type v-type true))
  (visit-list-key-range
    [db list-name visitor k-range k-type v-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/visit-list-key-range db list-name visitor k-range k-type
                                   v-type raw-pred?))
      (datalevin.interface/visit-list-key-range native list-name visitor
                                                k-range k-type v-type
                                                raw-pred?)))
  (visit-list-range
    [db list-name visitor k-range k-type v-range v-type]
    (.visit-list-range db list-name visitor k-range k-type v-range v-type true))
  (visit-list-range
    [db list-name visitor k-range k-type v-range v-type raw-pred?]
    (if (kv-wal-enabled? db)
      (with-wal-overlay-scan db
        (scan/visit-list-range db list-name visitor k-range k-type
                               v-range v-type raw-pred?))
      (datalevin.interface/visit-list-range native list-name visitor
                                            k-range k-type v-range v-type
                                            raw-pred?)))

  IAdmin
  (re-index [db opts] (datalevin.interface/re-index native opts))
  (re-index
    [db schema opts]
    (datalevin.interface/re-index native schema opts)))

(defn- mark-wal-writing
  [db]
  (let [m (assoc (or (meta db) {}) :wal-writing? true)]
    (KV. (.-native db) m)))

(defn wrap-kv
  [db]
  (if (instance? KV db)
    db
    (let [wrapped (KV. db nil)]
      (try
        (init-kv-wal-runtime! wrapped)
        wrapped
        (catch Exception e
          (u/raise "Fail to open database: " e {:dir (i/env-dir wrapped)}))))))

(defn- wrap-like
  [this db]
  (let [wrapped (wrap-kv db)]
    (if-let [m (.meta ^IObj this)]
      (.withMeta ^IObj wrapped m)
      wrapped)))

(defn unwrap-kv
  [db]
  (if (instance? KV db)
    (.-native ^KV db)
    db))

(defmethod l/open-kv :cpp
  ([dir]
   (wrap-kv (cpp/open-cpp-kv dir {})))
  ([dir opts]
   (wrap-kv (cpp/open-cpp-kv dir opts))))
