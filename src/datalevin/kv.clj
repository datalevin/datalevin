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
   [datalevin.scan :as scan]
   [datalevin.util :as u]
   [datalevin.validate :as vld]
   [datalevin.wal :as wal])
  (:import
   [clojure.lang IObj]
   [datalevin.binding.cpp Rtx]
   [datalevin.cpp Txn]
   [datalevin.lmdb KVTxData]
   [java.nio ByteBuffer BufferOverflowException]
   [java.nio.channels FileChannel]
   [java.nio.charset StandardCharsets]))

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

(defn- overlay-active?
  [_db _dbi-name]
  false)

(defn- dbi-opts!
  [db dbi-name]
  (if (= dbi-name c/kv-info)
    ;; kv-info DBI is always opened by cpp/open-kv* but not tracked in :dbis.
    {:flags #{} :validate-data? false}
    (or (get-in @(l/kv-info db) [:dbis dbi-name])
        (u/raise (str dbi-name " is not open") {}))))

(defn- dbi-dupsort?
  [db dbi-name]
  (boolean (get-in (dbi-opts! db dbi-name) [:flags :dupsort])))

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
         kt (normalize-kv-type (.-kt tx))]
     {:op (.-op tx)
      :dbi dbi-name
      :k (.-k tx)
      :v (.-v tx)
      :kt kt
      :vt (.-vt tx)
      :flags (.-flags tx)
      :dupsort? (dbi-dupsort? db dbi-name)
      :k-bytes (encode-kv-bytes (.-k tx) kt)
      :dbi-bytes (or cached-dbi-bytes
                     (.getBytes ^String dbi-name StandardCharsets/UTF_8))})))

(defn- run-writer-step!
  [step f]
  (vld/check-failpoint step :before)
  (vld/check-failpoint step :during)
  (let [ret (f)]
    (vld/check-failpoint step :after)
    ret))

(defn- wal-transact-one-shot!
  [db dbi-name txs k-type v-type]
  (let [info (l/kv-info db)
        write-txn (l/write-txn db)]
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
            wal-entry (when (seq ops)
                        (run-writer-step!
                         :step-3
                         #(wal/append-kv-wal-record!
                           db ops (System/currentTimeMillis))))
            wal-id (:wal-id wal-entry)]
        (when wal-id
          (run-writer-step! :step-4 (fn [] nil))
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
        :transacted)
      (catch Exception e
        (wal/refresh-kv-wal-info! db)
        (wal/refresh-kv-wal-meta-info! db)
        (if-let [edata (ex-data e)]
          (u/raise "Fail to transact to LMDB: " e edata)
          (u/raise "Fail to transact to LMDB: " e {}))))))

(defn- wal-transact-open-txn!
  [db dbi-name txs k-type v-type]
  (let [info (l/kv-info db)
        write-txn (l/write-txn db)
        ^Rtx rtx @write-txn]
    (if (nil? rtx)
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
              ops (if max-val-op (conj ops max-val-op) ops)]
          (when (seq ops)
            ;; Defer private overlay materialization until a read needs it.
            ;; Write-only txns then avoid temp private overlay env setup.
            (vswap! (.-wal-ops rtx) into ops))
          :transacted)
        (catch Exception e
          (if-let [edata (ex-data e)]
            (u/raise "Fail to transact to LMDB: " e edata)
            (u/raise "Fail to transact to LMDB: " e {})))))))

(defn- wal-close-transact!
  [db]
  (let [info (l/kv-info db)
        write-txn (l/write-txn db)]
    (if-let [^Rtx wtxn @write-txn]
      (if-let [^Txn txn (.-txn wtxn)]
        (let [aborted? @(.-aborted? wtxn)
              ops (when-not aborted?
                    (seq @(.-wal-ops wtxn)))
              _overlay-private (some-> (l/private-overlay wtxn) deref)]
          (try
            (let [wal-entry (when (seq ops)
                              (run-writer-step!
                               :step-3
                               #(wal/append-kv-wal-record!
                                 db (vec ops)
                                 (System/currentTimeMillis))))
                  wal-id (:wal-id wal-entry)]
              (when wal-id
                (locking write-txn
                  (run-writer-step! :step-4 (fn [] nil))
                  (run-writer-step! :step-5 (fn [] nil))
                  (run-writer-step! :step-6 (fn [] nil))
                  (run-writer-step! :step-7 (fn [] nil))))
              (.abort txn)
              (vreset! write-txn nil)
              (.close txn)
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
              (when (.-wal-ops wtxn)
                (vreset! (.-wal-ops wtxn) []))
              (if aborted? :aborted :committed))
            (catch Exception e
              (wal/refresh-kv-wal-info! db)
              (wal/refresh-kv-wal-meta-info! db)
              (when-let [^Txn t (.-txn wtxn)]
                (try
                  (.close t)
                  (catch Exception _ nil)))
              (vreset! write-txn nil)
              (if-let [edata (ex-data e)]
                (u/raise "Fail to close read/write transaction in LMDB: "
                         e edata)
                (u/raise "Fail to close read/write transaction in LMDB: "
                         e {})))))
        (u/raise "Calling `close-transact-kv` without opening" {}))
      (u/raise "Calling `close-transact-kv` without opening" {}))))

(defn- wal-abort-transact!
  [db]
  (let [info (l/kv-info db)
        write-txn (l/write-txn db)]
    (when-let [^Rtx wtxn @write-txn]
      (vreset! (.-aborted? wtxn) true)
      (when (.-wal-ops wtxn)
        (vreset! (.-wal-ops wtxn) []))
      (vreset! write-txn wtxn)
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
      (vswap! info dissoc :wal-runtime-ready?))))

(defn- init-kv-wal-runtime!
  [db]
  (let [info (l/kv-info db)]
    (when (:kv-wal? @info)
      (locking info
        (when (and (:kv-wal? @info) (not (:wal-runtime-ready? @info)))
          (wal/refresh-kv-wal-info! db)
          (wal/refresh-kv-wal-meta-info! db)
          (wal/start-scheduled-wal-checkpoint info db l/vector-indices)
          (vswap! info assoc :wal-runtime-ready? true))))))

(defn- flush-kv-indexer-if-wal!
  [db]
  (when (and (kv-wal-enabled? db) (not c/*bypass-wal*))
    (i/flush-kv-indexer! db)))

(defn- sync-kv-overlay-on-miss!
  [db]
  (when (and (kv-wal-enabled? db)
             (not c/*bypass-wal*)
             (nil? @(l/write-txn db)))
    false))

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
    (l/iterate-key-sample native rtx cur indices budget step k-range k-type))
  (iterate-list [_ rtx cur k-range k-type v-range v-type]
    (if (kv-wal-enabled? db)
      (wal-iterate-list db db native rtx cur k-range k-type v-range v-type)
      (l/iterate-list native rtx cur k-range k-type v-range v-type)))
  (iterate-list-sample [_ rtx cur indices budget step k-range k-type]
    (l/iterate-list-sample native rtx cur indices budget step k-range k-type))
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

(defn- info-snapshot
  [info-src]
  (if (instance? clojure.lang.IDeref info-src) @info-src info-src))

(defn- wal-iterate-key
  [_db _lmdb dbi rtx cur k-range k-type]
  (let [dbi* (native-dbi dbi)]
    (l/iterate-key dbi* rtx cur k-range k-type)))

(defn- wal-iterate-list
  [_db _lmdb dbi rtx cur k-range k-type v-range v-type]
  (let [dbi* (native-dbi dbi)]
    (l/iterate-list dbi* rtx cur k-range k-type v-range v-type)))

(defn- wal-iterate-list-key-range-val-full
  [_db _lmdb dbi rtx cur k-range k-type]
  (let [dbi* (native-dbi dbi)]
    (l/iterate-list-key-range-val-full dbi* rtx cur k-range k-type)))

(defn- wal-iterate-list-val-full
  [_db _lmdb dbi rtx cur]
  (let [dbi* (native-dbi dbi)]
    (l/iterate-list-val-full dbi* rtx cur)))

(defn- wal-iterate-kv
  [_db _lmdb dbi rtx cur k-range k-type v-type]
  (let [dbi* (native-dbi dbi)]
    (l/iterate-kv dbi* rtx cur k-range k-type v-type)))

(defmacro with-wal-overlay-scan
  [db & body]
  `(binding [scan/*iterate-key* (partial wal-iterate-key ~db)
             scan/*iterate-list* (partial wal-iterate-list ~db)
             scan/*iterate-list-key-range-val-full*
             (partial wal-iterate-list-key-range-val-full ~db)
             scan/*iterate-list-val-full* (partial wal-iterate-list-val-full ~db)
             scan/*iterate-kv* (partial wal-iterate-kv ~db)]
     ~@body))

(deftype KV [native ^:unsynchronized-mutable meta]
  l/IWriting
  (writing? [_] (l/writing? native))
  (write-txn [_] (l/write-txn native))
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
    (wrap-like this (i/open-transact-kv native)))

  (get-value [this dbi-name k]
    (.get-value this dbi-name k :data :data true))
  (get-value [this dbi-name k k-type]
    (.get-value this dbi-name k k-type :data true))
  (get-value [this dbi-name k k-type v-type]
    (.get-value this dbi-name k k-type v-type true))
  (get-value [this dbi-name k k-type v-type ignore-key?]
    (scan/get-value this dbi-name k k-type v-type ignore-key?))

  (get-rank [this dbi-name k]
    (.get-rank this dbi-name k :data))
  (get-rank [this dbi-name k k-type]
    (scan/get-rank this dbi-name k k-type))

  (get-by-rank [this dbi-name rank]
    (.get-by-rank this dbi-name rank :data :data true))
  (get-by-rank [this dbi-name rank k-type]
    (.get-by-rank this dbi-name rank k-type :data true))
  (get-by-rank [this dbi-name rank k-type v-type]
    (.get-by-rank this dbi-name rank k-type v-type true))
  (get-by-rank [this dbi-name rank k-type v-type ignore-key?]
    (scan/get-by-rank this dbi-name rank k-type v-type ignore-key?))

  (sample-kv [this dbi-name n]
    (.sample-kv this dbi-name n :data :data true))
  (sample-kv [this dbi-name n k-type]
    (.sample-kv this dbi-name n k-type :data true))
  (sample-kv [this dbi-name n k-type v-type]
    (.sample-kv this dbi-name n k-type v-type true))
  (sample-kv [this dbi-name n k-type v-type ignore-key?]
    (scan/sample-kv this dbi-name n k-type v-type ignore-key?))

  (key-range-count [this dbi-name k-range]
    (.key-range-count this dbi-name k-range :data))
  (key-range-count [this dbi-name k-range k-type]
    (i/key-range-count native dbi-name k-range k-type))

  (key-range-list-count [this dbi-name k-range k-type]
    (i/key-range-list-count native dbi-name k-range k-type))

  (visit-key-sample [this dbi-name indices budget step visitor k-range k-type]
    (.visit-key-sample this dbi-name indices budget step visitor k-range k-type
                       true))
  (visit-key-sample
    [this dbi-name indices budget step visitor k-range k-type raw-pred?]
    (i/visit-key-sample native dbi-name indices budget step visitor
                        k-range k-type raw-pred?))

  (range-count [this dbi-name k-range]
    (.range-count this dbi-name k-range :data))
  (range-count [this dbi-name k-range k-type]
    (i/range-count native dbi-name k-range k-type))

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
          (i/transact-kv native dbi-name txs k-type v-type)

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
    (let [res (wal/flush-kv-indexer! this upto-wal-id)]
      (locking (l/write-txn this)
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
    ;; In WAL mode, clear-dbi can be used to clear base LMDB state while
    ;; preserving committed overlay visibility (e.g. replay/recovery tests).
    ;; Do not force synchronous WAL catch-up here.
    (datalevin.interface/clear-dbi native dbi-name))
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
  (dbi-opts [db dbi-name] (datalevin.interface/dbi-opts native dbi-name))
  (drop-dbi
    [db dbi-name]
    (flush-kv-indexer-if-wal! db)
    (let [existing (datalevin.interface/dbi-opts native dbi-name)
          ret      (datalevin.interface/drop-dbi native dbi-name)]
      (when (and existing (not= dbi-name c/kv-info))
        (.transact-kv db c/kv-info
                      [[:del [:dbis dbi-name]]]
                      [:keyword :string]))
      ret))
  (entries
    [db dbi-name]
    (datalevin.interface/entries native dbi-name))
  (env-dir [db] (datalevin.interface/env-dir native))
  (env-opts [db] (datalevin.interface/env-opts native))
  (get-dbi
    [db dbi-name]
    (wrap-dbi db (datalevin.interface/get-dbi native dbi-name)))
  (get-dbi
    [db dbi-name create?]
    (wrap-dbi db
              (datalevin.interface/get-dbi native dbi-name create?)))
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
  (list-dbis [db] (datalevin.interface/list-dbis native))
  (max-val-size [db] (datalevin.interface/max-val-size native))
  (open-dbi [db dbi-name] (.open-dbi db dbi-name nil))
  (open-dbi
    [db dbi-name opts]
    (let [existing (datalevin.interface/dbi-opts native dbi-name)
          ret      (wrap-dbi db
                             (datalevin.interface/open-dbi native dbi-name
                                                           opts))]
      (when (and (nil? existing) (not= dbi-name c/kv-info))
        (let [dbi-opts (datalevin.interface/dbi-opts native dbi-name)]
          (.transact-kv db
                        [(l/kv-tx :put c/kv-info [:dbis dbi-name] dbi-opts
                                  [:keyword :string])])))
      ret))
  (open-list-dbi
    [db list-name]
    (.open-list-dbi db list-name nil))
  (open-list-dbi
    [db list-name opts]
    (let [existing (datalevin.interface/dbi-opts native list-name)
          ret      (wrap-dbi db
                             (datalevin.interface/open-list-dbi native list-name
                                                                opts))]
      (when (and (nil? existing) (not= list-name c/kv-info))
        (let [dbi-opts (datalevin.interface/dbi-opts native list-name)]
          (.transact-kv db
                        [(l/kv-tx :put c/kv-info [:dbis list-name] dbi-opts
                                  [:keyword :string])])))
      ret))
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
  (stat [db] (datalevin.interface/stat native))
  (stat
    [db dbi-name]
    (if (and dbi-name (kv-wal-enabled? db))
      (do
        (flush-kv-indexer-if-wal! db)
        (datalevin.interface/stat native dbi-name))
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
    (i/list-count native dbi-name k kt))

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
    (i/list-range-count native dbi-name k-range k-type v-range v-type))

  (visit-list-sample
    [this dbi-name indices budget step visitor k-range k-type v-type]
    (.visit-list-sample this dbi-name indices budget step visitor
                        k-range k-type v-type true))
  (visit-list-sample
    [this dbi-name indices budget step visitor k-range k-type v-type
     raw-pred?]
    (i/visit-list-sample native dbi-name indices budget step visitor
                         k-range k-type v-type raw-pred?))
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
    (datalevin.interface/list-dbi? native dbi-name))
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
