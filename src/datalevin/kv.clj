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
  "KV-layer helpers for txn-log APIs and floor-provider bookkeeping."
  (:require
   [datalevin.bits :as b]
   [datalevin.binding.cpp :as cpp]
   [datalevin.constants :as c]
   [datalevin.custom-kv :as custom-kv]
   [datalevin.interface :as i]
   [datalevin.prepared :as prepared]
   [datalevin.read-encode :as enc]
   [datalevin.scan :as scan]
   [datalevin.kv.snapshot :refer [list-snapshot-entries]]
   [datalevin.kv.retention :refer [delete-txlog-segment!
                                   gc-txlog-segments-local!
                                   txlog-log-dbi-drop!
                                   txlog-log-dbi-registration!
                                   txlog-retention-state-local
                                   txlog-retention-state-map]]
   [datalevin.kv.scheduler :as scheduler]
   [datalevin.kv.txlog :as kvtx :refer [close-failed-open!
                                        close-txlog-state!
                                        close-with-txlog!
                                        read-commit-marker-state
                                        refresh-runtime-marker-revision!
                                        transact-with-txlog!
                                        txlog-reset-pending!
                                        txlog-runtime-state
                                        verify-commit-marker-state
                                        create-snapshot-now!
                                        force-lmdb-sync-now!
                                        recover-from-snapshot-open!
                                        txlog-clear-replica-floor-state!
                                        txlog-clear-snapshot-floor-state!
                                        txlog-config-enabled? txlog-force-sync!
                                        txlog-pin-backup-floor-state!
                                        txlog-rollout-mode
                                        txlog-rollout-watermarks
                                        txlog-records
                                        txlog-unpin-backup-floor-state!
                                        txlog-update-replica-floor-state!
                                        txlog-update-snapshot-floor-state!
                                        txlog-watermarks-map
                                        txlog-write-path-enabled?
                                        with-runtime-txlog-state-guard
                                        with-write-txn-lock-before-runtime-txlog-state]]
   [datalevin.lmdb :as l]
   [datalevin.txlog :as txlog]
   [datalevin.txlog.transfer :as transfer]
   [datalevin.tx-group.compat :as group]
   [datalevin.util :refer [deftype+ raise]])
  (:import [java.util.concurrent.atomic AtomicReference AtomicBoolean]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function Function]))

(declare ->KVLMDB)

(declare raw-lmdb)

(defn- independent-control [db]
  (when-let [info (i/kv-info db)] (:independent-control @info)))

(defn write-batch-delay-nanos
  "Optional idle collection window, shared by embedded KV and Datalog writes."
  ^long [db]
  (* 1000 (long (or (:write-batch-delay-us (l/read-env-opts db)) 0))))

(defn write-group
  "Return the admission queue for eligible standalone writes.
  The server may opt HA stores in when it retains both HA guards around the
  physical commit. Embedded HA, explicit transactions and shared WAL stay out."
  ([db kind] (write-group db kind false))
  ([db kind ha-guarded?]
   (when (and group/*enabled?*
              (nil? cpp/*before-write-commit-fn*)
              (nil? kvtx/*after-txlog-append-fn*)
              (not (l/writing? db))
              (not (Thread/holdsLock (l/write-txn db))))
     (let [info @(i/kv-info db)
           state (:txlog-state info)]
       (when (and (or (nil? (:ha-mode info)) ha-guarded?)
                  (or (and state
                           (or (#{:strict :relaxed} (:durability-profile state))
                               (and (vector? kind) (= :datalog (first kind))
                                    (= :extra (:durability-profile state))))
                           (not (:wal-shared? state))
                           (kvtx/txlog-write-path-enabled? db))
                      (and (#{:kv :datalog} (if (vector? kind) (first kind) kind))
                           (not (:wal? info))
                           (not (:temp? info))
                           (not-any? #{:nosync :nometasync :mapasync :inmemory}
                                     (:flags info)))))
         (let [^ConcurrentHashMap groups (if state (:write-groups state)
                                            (:write-groups info))]
           (.computeIfAbsent groups kind
                             (reify Function
                               (apply [_ _]
                                 (group/create
                                  (or (:write-batch-size info)
                                      (txlog/group-commit info))))))))))))

(defn grouped-write!
  "Execute a standalone KV operation in a group under its durability policy."
  [db op]
  (if-let [control (independent-control db)]
    (do (i/check-ready db)
        (if (l/writing? db) (op db) ((:body! control) op nil)))
    (if-let [g (write-group db :kv)]
    (group/submit! g
                   (fn [execute]
                     (l/with-transaction-kv [tx db]
                       (execute tx)))
                   op
                   (write-batch-delay-nanos db))
      (l/with-transaction-kv [tx db] (op tx)))))

(def ensure-txlog-ready! kvtx/ensure-txlog-ready!)

(def transact-kv-without-txlog! kvtx/transact-kv-without-txlog!)

(def mirror-replayed-txlog-record! kvtx/mirror-replayed-txlog-record!)
(def mirror-replayed-txlog-records! kvtx/mirror-replayed-txlog-records!)
(def batchable-replay-record? kvtx/batchable-replay-record?)

(def replay-txlog-rows! kvtx/replay-txlog-rows!)



(defn open-tx-log
  ([db from-lsn]
   (i/open-tx-log db from-lsn))
  ([db from-lsn upto-lsn]
   (i/open-tx-log db from-lsn upto-lsn)))

(defn open-tx-log-rows
  ([db from-lsn]
   (open-tx-log-rows db from-lsn nil))
  ([db from-lsn upto-lsn]
   (if-let [state (or (txlog/state db)
                      (when (txlog-write-path-enabled? db)
                        (ensure-txlog-ready! db)))]
     (do
       (txlog/refresh-shared-state! state)
       (txlog/select-open-record-rows
        (txlog-records state from-lsn upto-lsn)
        from-lsn
        upto-lsn))
     (if (txlog-config-enabled? db)
       []
       (txlog/select-open-record-rows
        (txlog-records (txlog/enabled-state db) from-lsn upto-lsn)
        from-lsn
        upto-lsn)))))

(defn ^:no-doc open-tx-log-batch
  "Serve encoded records for internal replication without materializing rows."
  [db from-lsn upto-lsn]
  (if-let [state (or (txlog/state db)
                     (when (txlog-write-path-enabled? db)
                       (ensure-txlog-ready! db)))]
    (do
      (txlog/refresh-shared-state! state)
      (kvtx/txlog-record-batch state from-lsn upto-lsn))
    (if (txlog-config-enabled? db)
      (transfer/encode-batch [])
      (kvtx/txlog-record-batch (txlog/enabled-state db) from-lsn upto-lsn))))

(defn force-txlog-sync!
  [db]
  (i/force-txlog-sync! db))

(defn txlog-watermarks
  [db]
  (if-let [control (independent-control db)]
    ((:watermarks control))
    (kvtx/txlog-watermarks db)))

(defn force-lmdb-sync!
  [db]
  (i/force-lmdb-sync! db))

(defn create-snapshot!
  [db]
  (i/create-snapshot! db))

(defn list-snapshots
  [db]
  (i/list-snapshots db))

(defn snapshot-scheduler-state
  [db]
  (i/snapshot-scheduler-state db))

(defn read-commit-marker
  [db]
  (i/read-commit-marker db))

(defn verify-commit-marker!
  [db]
  (i/verify-commit-marker! db))

(defn txlog-retention-state
  [db]
  (if-let [state (txlog-retention-state-map db nil false)]
    (dissoc state :gc-target-segments)
    (i/txlog-retention-state db)))

(defn gc-txlog-segments!
  ([db]
   (gc-txlog-segments! db nil))
  ([db retain-floor-lsn]
   (if-let [before (txlog-retention-state-map db retain-floor-lsn true)]
     (let [targets (:gc-target-segments before)
           deleted (mapv delete-txlog-segment! targets)
           deleted-bytes (reduce (fn [acc {:keys [bytes]}]
                                   (+ ^long acc ^long bytes))
                                 0 targets)
           _ (when-let [state (txlog/state db)]
               (txlog/note-gc-deleted-bytes! state deleted-bytes))
           after (txlog-retention-state-map db retain-floor-lsn false)]
       {:ok? true
        :deleted-count (count deleted)
        :deleted-bytes deleted-bytes
        :deleted-segment-ids (mapv :segment-id targets)
        :deleted-segments deleted
        :operator-retain-floor-lsn
        (get-in before [:floors :operator-retain-floor-lsn])
        :before (dissoc before :gc-target-segments)
        :after (dissoc after :gc-target-segments)})
     (i/gc-txlog-segments! db retain-floor-lsn))))

(defn txlog-update-snapshot-floor!
  ([db snapshot-lsn]
   (txlog-update-snapshot-floor-state! db snapshot-lsn nil))
  ([db snapshot-lsn previous-snapshot-lsn]
   (txlog-update-snapshot-floor-state! db snapshot-lsn previous-snapshot-lsn)))

(defn txlog-clear-snapshot-floor!
  [db]
  (txlog-clear-snapshot-floor-state! db))

(defn txlog-update-replica-floor!
  [db replica-id applied-lsn]
  (txlog-update-replica-floor-state! db replica-id applied-lsn))

(defn txlog-clear-replica-floor!
  [db replica-id]
  (txlog-clear-replica-floor-state! db replica-id))

(defn txlog-pin-backup-floor!
  ([db pin-id floor-lsn]
   (txlog-pin-backup-floor-state! db pin-id floor-lsn nil))
  ([db pin-id floor-lsn expires-ms]
   (txlog-pin-backup-floor-state! db pin-id floor-lsn expires-ms)))

(defn txlog-unpin-backup-floor!
  [db pin-id]
  (txlog-unpin-backup-floor-state! db pin-id))

(declare wrap-lmdb)

(defmacro def-read-kv-forwarders
  "Expand to KVLMDB methods that forward to `custom-kv/read-kv`.

  `store` is the receiver symbol and `lmdb` the wrapped LMDB expression
  passed to `read-kv`. Each spec is `(method-name [args...])`, with the
  receiver omitted, and expands to
  `(method-name [store args...] (custom-kv/read-kv :method-name store lmdb args...))`."
  [store lmdb & specs]
  (cons 'do
        (map (fn [[mname args]]
               (list mname (into [store] args)
                     (concat ['custom-kv/read-kv (keyword (name mname))
                              store lmdb]
                             args)))
             specs)))

(deftype+ KVLMDB [db handle]
  l/IWriting
  (writing? [_] (l/writing? db))
  (write-txn [_] (l/write-txn db))
  (mark-write [_] (wrap-lmdb (l/mark-write db)))
  (reset-write [_] (wrap-lmdb (l/reset-write db)))

  i/IList
  (del-list-items
    [this list-name k k-type]
    (i/transact-kv this [(l/kv-tx :del list-name k k-type)]))
  (del-list-items
    [this list-name k vs k-type v-type]
    (i/transact-kv this [(l/kv-tx :del-list list-name k vs k-type v-type)]))
  (list-dbi? [_ dbi-name] (i/list-dbi? db dbi-name))
  (put-list-items
    [this list-name k vs k-type v-type]
    (i/transact-kv this [(l/kv-tx :put-list list-name k vs k-type v-type)]))

  (def-read-kv-forwarders this db
    (get-list [list-name k k-type v-type])
    (in-list? [list-name k v k-type v-type])
    (list-count [list-name k k-type])
    (list-range [list-name k-range k-type v-range v-type])
    (list-range-count [list-name k-range k-type])
    (list-range-filter [list-name pred k-range k-type v-range v-type])
    (list-range-filter [list-name pred k-range k-type v-range v-type raw-pred?])
    (list-range-filter-count [list-name pred k-range k-type v-range v-type])
    (list-range-filter-count
      [list-name pred k-range k-type v-range v-type raw-pred?])
    (list-range-first [list-name k-range k-type v-range v-type])
    (list-range-first-n [list-name n k-range k-type v-range v-type])
    (list-range-keep [list-name pred k-range k-type v-range v-type])
    (list-range-keep [list-name pred k-range k-type v-range v-type raw-pred?])
    (list-range-some [list-name pred k-range k-type v-range v-type])
    (list-range-some [list-name pred k-range k-type v-range v-type raw-pred?])
    (near-list [list-name k v k-type v-type])
    (visit-list [list-name visitor k k-type])
    (visit-list [list-name visitor k k-type v-type])
    (visit-list [list-name visitor k k-type v-type raw-pred?])
    (visit-list-key-range [list-name visitor k-range k-type v-type])
    (visit-list-key-range [list-name visitor k-range k-type v-type raw-pred?])
    (visit-list-range [list-name visitor k-range k-type v-range v-type])
    (visit-list-range [list-name visitor k-range k-type v-range v-type raw-pred?])
    (visit-list-sample [list-name indices visitor k-range k-type v-type])
    (visit-list-sample
      [list-name indices visitor k-range k-type v-type raw-pred?]))

  i/IAdmin
  (re-index [_ opts] (i/re-index db opts))
  (re-index [_ schema opts] (i/re-index db schema opts))

  i/ITxLog
  (txlog-watermarks
    [_]
    (if-let [control (independent-control db)]
      ((:watermarks control))
      (if-let [state (txlog/state db)]
      (txlog-watermarks-map db state)
      (if (txlog-config-enabled? db)
        (txlog-rollout-watermarks db (txlog-rollout-mode db))
        {:wal? false}))))

  (open-tx-log
    [this from-lsn]
    (.open-tx-log this from-lsn nil))
  (open-tx-log
    [_ from-lsn upto-lsn]
    (if-let [state (or (txlog/state db)
                       (when (txlog-write-path-enabled? db)
                         (ensure-txlog-ready! db)))]
      (do
        (txlog/refresh-shared-state! state)
        (txlog/select-open-records
          (txlog-records state from-lsn upto-lsn)
          from-lsn
          upto-lsn))
      (if (txlog-config-enabled? db)
        []
        (txlog/select-open-records
          (txlog-records (txlog/enabled-state db) from-lsn upto-lsn)
          from-lsn
          upto-lsn))))

  (force-txlog-sync!
    [_]
    (if-let [control (independent-control db)]
      ((:force! control))
      (with-runtime-txlog-state-guard
      db
      (fn []
        (cond
          (not (txlog-config-enabled? db))
          (txlog/enabled-state db)

          (not (txlog-write-path-enabled? db))
          (let [rollout-mode (txlog-rollout-mode db)]
            {:synced?    false
             :skipped?   true
             :reason     :rollback
             :watermarks (txlog-rollout-watermarks db rollout-mode)})

          :else
          (let [state (txlog/enabled-state db)]
            (assoc (txlog-force-sync! state)
                   :watermarks (txlog-watermarks-map db state))))))))

  (force-lmdb-sync!
    [_]
    (if (txlog-config-enabled? db)
      (do
        (force-lmdb-sync-now! db)
        {:synced?    true
         :watermarks (if-let [state (txlog/state db)]
                       (txlog-watermarks-map db state)
                       (txlog-rollout-watermarks db
                                                 (txlog-rollout-mode db)))})
      (txlog/enabled-state db)))

  (create-snapshot!
    [_]
    ;; Snapshot creation updates backup-pin floor metadata before copying the
    ;; environment, so it participates in the same write-capable lock ordering
    ;; as replica-floor bookkeeping and transaction close.
    (if-let [control (independent-control db)]
      ((:snapshot! control))
      (with-write-txn-lock-before-runtime-txlog-state
      db
      (fn []
        (if (txlog-write-path-enabled? db)
          (create-snapshot-now! db)
          (if (txlog-config-enabled? db)
            (let [rollout-mode (txlog-rollout-mode db)]
              {:ok?        false
               :skipped?   true
               :reason     :rollback
               :watermarks (txlog-rollout-watermarks db rollout-mode)})
            (txlog/enabled-state db)))))))

  (list-snapshots [_]
    (if-let [control (independent-control db)]
      ((:snapshots control))
      (list-snapshot-entries db)))

  (snapshot-scheduler-state
    [_]
    (if-let [control (independent-control db)]
      ((:scheduler-state control))
      (scheduler/snapshot-scheduler-state-map db)))

  (read-commit-marker
    [_]
    (if-let [state (txlog/state db)]
      (do
        (refresh-runtime-marker-revision! db state)
        (assoc (read-commit-marker-state db)
               :commit-marker? (boolean (:commit-marker? state))
               :marker-revision (long @(:marker-revision state))))
      {:commit-marker? false
       :slot-a         nil
       :slot-b         nil
       :current        nil}))

  (verify-commit-marker!
    [_]
    (with-runtime-txlog-state-guard
      db
      (fn []
        (if-let [state (txlog/state db)]
          (verify-commit-marker-state db state)
          (if (txlog-config-enabled? db)
            (let [rollout-mode (txlog-rollout-mode db)]
              {:ok?        false
               :skipped?   true
               :reason     :rollback
               :watermarks (txlog-rollout-watermarks db rollout-mode)})
            (verify-commit-marker-state db (txlog/enabled-state db)))))))

  (txlog-retention-state [_] (txlog-retention-state-local db))

  (gc-txlog-segments! [this] (.gc-txlog-segments! this nil))
  (gc-txlog-segments!
    [_ retain-floor-lsn]
    (gc-txlog-segments-local! db retain-floor-lsn))

  (txlog-update-snapshot-floor!
    [this snapshot-lsn]
    (.txlog-update-snapshot-floor! this snapshot-lsn nil))
  (txlog-update-snapshot-floor!
    [_ snapshot-lsn previous-snapshot-lsn]
    (txlog-update-snapshot-floor! db snapshot-lsn previous-snapshot-lsn))

  (txlog-clear-snapshot-floor! [_] (txlog-clear-snapshot-floor! db))

  (txlog-update-replica-floor!
    [_ replica-id applied-lsn]
    (txlog-update-replica-floor! db replica-id applied-lsn))

  (txlog-clear-replica-floor!
    [_ replica-id]
    (txlog-clear-replica-floor! db replica-id))

  (txlog-pin-backup-floor!
    [this pin-id floor-lsn]
    (.txlog-pin-backup-floor! this pin-id floor-lsn nil))
  (txlog-pin-backup-floor!
    [_ pin-id floor-lsn expires-ms]
    (txlog-pin-backup-floor! db pin-id floor-lsn expires-ms))

  (txlog-unpin-backup-floor!
    [_ pin-id]
    (txlog-unpin-backup-floor! db pin-id))

  i/ILMDB
  (open-transact-kv
    [_]
    (when-let [control (independent-control db)]
      ((:unsupported! control) :manual-transaction))
    (->KVLMDB (kvtx/open-transact-with-txlog! db) nil))
  (abort-transact-kv
    [_]
    (when (txlog-config-enabled? db)
      (txlog-reset-pending! (i/kv-info db)))
    (i/abort-transact-kv db))
  (check-ready [_]
    (when (and handle (.get ^AtomicBoolean (:closed? handle)))
      (raise "LMDB handle is closed." {:type :lmdb/closed}))
    (i/check-ready db))
  (clear-dbi
    [this dbi-name]
    (custom-kv/guard-internal! db dbi-name)
    (if-let [control (independent-control db)]
      (do
        (i/check-ready this)
        (if (l/writing? db)
          (i/transact-kv db [(l/kv-tx :clear dbi-name nil nil :raw :raw)])
          ((:body! control)
           (fn [tx] (i/clear-dbi tx dbi-name)) {}))
        nil)
      (if (custom-kv/custom-dbi? db dbi-name)
        (custom-kv/clear! this db dbi-name)
        (i/clear-dbi db dbi-name))))

  (close-kv
    [_]
    (when (and (l/writing? db) (independent-control db))
      ((:unsupported! (independent-control db)) :close-inside-transaction))
    (if handle
      ((:close! handle))
      (try
      (i/close-kv db)
      (finally
        ;; A fenced native call can outlive a close deadline. Its WAL channels
        ;; and runtime must stay registered until native teardown succeeds.
        (when (i/closed-kv? db) (close-txlog-state! db))))))
  (close-transact-kv
    [_]
    (with-write-txn-lock-before-runtime-txlog-state
      db
      (fn []
        (if (txlog-write-path-enabled? db)
          (if-let [state (txlog-runtime-state db)]
            (close-with-txlog! db state)
            (i/close-transact-kv db))
          (i/close-transact-kv db)))))
  (closed-kv? [_] (or (and handle (.get ^AtomicBoolean (:closed? handle)))
                      (i/closed-kv? db)))
  (copy [this dest] (.copy this dest false))
  (copy
    [this dest compact?]
    (kvtx/txlog-copy-with-backup-pin! this db dest compact?))

  (dbi-opts [_ dbi-name] (i/dbi-opts db dbi-name))
  (drop-dbi
    [this dbi-name]
    (when-let [control (independent-control db)]
      ((:unsupported! control) :drop-dbi))
    (custom-kv/guard-internal! db dbi-name)
    (locking (l/write-txn db)
      (when (and (custom-kv/custom-dbi? db dbi-name) (some? @(l/write-txn db)))
        (raise "Drop a custom DBI outside an explicit transaction"
               {:error :custom-type/drop-transaction :dbi dbi-name}))
      (when (custom-kv/custom-dbi? db dbi-name)
        (custom-kv/clear! this db dbi-name))
      (let [before (try
                     (i/dbi-opts db dbi-name)
                     (catch Exception _ nil))
            res    (i/drop-dbi db dbi-name)]
        (txlog-log-dbi-drop! this db dbi-name before)
        res)))
  (entries [_ dbi-name] (i/entries db dbi-name))
  (env-dir [_] (i/env-dir db))
  (kv-info [_] (i/kv-info db))
  (env-opts [_] (merge (i/env-opts db) (:options (independent-control db))))
  (get-dbi [_ dbi-name] (i/get-dbi db dbi-name))
  (get-dbi [_ dbi-name create?] (i/get-dbi db dbi-name create?))
  (get-env-flags [_] (i/get-env-flags db))
  (get-rtx [_] (i/get-rtx db))
  (key-compressor [_] (i/key-compressor db))
  (list-dbis [_] (i/list-dbis db))
  (max-val-size [_] (i/max-val-size db))
  (open-dbi [this dbi-name] (.open-dbi this dbi-name nil))
  (open-dbi
    [this dbi-name opts]
    (if-let [control (independent-control db)]
      (do (.check-ready this) ((:open-dbi! control) dbi-name opts))
      (let [before   (try
                     (i/dbi-opts db dbi-name)
                     (catch Exception _ nil))
          prepared (if l/*raw-kv?* opts
                       (custom-kv/prepare-dbi! this db dbi-name opts))
          res      (i/open-dbi db dbi-name prepared)
          after    (i/dbi-opts db dbi-name)]
      (txlog-log-dbi-registration! this db dbi-name before after)
      res)))
  (open-list-dbi [this list-name] (.open-list-dbi this list-name nil))
  (open-list-dbi
    [this list-name opts]
    (if-let [control (independent-control db)]
      (do
        (i/check-ready this)
        (when-not (some #{:dupsort} (:flags (i/dbi-opts db list-name)))
          ((:unsupported! control) :catalog-mutation))
        ((:open-dbi! control) list-name opts))
      (let [before   (try
                       (i/dbi-opts db list-name)
                       (catch Exception _ nil))
            supplied (assoc opts :flags
                            (conj (set (or (:flags opts) (:flags before)
                                           c/default-dbi-flags))
                                  :dupsort))
            prepared (if l/*raw-kv?* supplied
                         (custom-kv/prepare-dbi! this db list-name supplied))
            res      (i/open-list-dbi db list-name prepared)
            after    (i/dbi-opts db list-name)]
        (txlog-log-dbi-registration! this db list-name before after)
        res)))
  (return-rtx [_ rtx] (i/return-rtx db rtx))
  (set-env-flags [_ ks on-off] (i/set-env-flags db ks on-off))
  (set-key-compressor [_ c] (i/set-key-compressor db c))
  (set-max-val-size [_ size] (i/set-max-val-size db size))
  (set-val-compressor [_ c] (i/set-val-compressor db c))
  (stat [_] (i/stat db))
  (stat [_ dbi-name] (i/stat db dbi-name))
  (sync [_] (i/sync db))
  (sync [_ force] (i/sync db force))
  (transact-kv [this txs] (.transact-kv this nil txs))
  (transact-kv
    [this dbi-name txs]
    (.transact-kv this dbi-name txs :data :data))
  (transact-kv
    [this dbi-name txs k-type]
    (.transact-kv this dbi-name txs k-type :data))
  (transact-kv
    [this dbi-name txs k-type v-type]
    (if-let [control (independent-control db)]
      (do (.check-ready this)
          (if (l/writing? db)
            (i/transact-kv db dbi-name txs k-type v-type)
            ((:transact! control) dbi-name txs k-type v-type)))
      (if-let [g (write-group db :kv)]
      (group/submit! g
                     (fn [execute]
                       (l/with-transaction-kv [tx this]
                         (execute tx)))
                     #(i/transact-kv % dbi-name txs k-type v-type)
                     (write-batch-delay-nanos db))
      (if (custom-kv/custom-txs? db dbi-name txs)
        (custom-kv/transact! this db dbi-name txs k-type v-type)
        (with-write-txn-lock-before-runtime-txlog-state
          db
          (fn []
            (if (txlog-write-path-enabled? db)
              (if-let [state (txlog-runtime-state db)]
                (transact-with-txlog! db state dbi-name txs k-type v-type)
                (i/transact-kv db dbi-name txs k-type v-type))
              (i/transact-kv db dbi-name txs k-type v-type))))))))
  (val-compressor [_] (i/val-compressor db))

  (def-read-kv-forwarders this db
    (get-by-rank [dbi-name rank])
    (get-by-rank [dbi-name rank k-type])
    (get-by-rank [dbi-name rank k-type v-type])
    (get-by-rank [dbi-name rank k-type v-type ignore-key?])
    (get-first [dbi-name k-range])
    (get-first [dbi-name k-range k-type])
    (get-first [dbi-name k-range k-type v-type])
    (get-first [dbi-name k-range k-type v-type ignore-key?])
    (get-first-n [dbi-name n k-range])
    (get-first-n [dbi-name n k-range k-type])
    (get-first-n [dbi-name n k-range k-type v-type])
    (get-first-n [dbi-name n k-range k-type v-type ignore-key?])
    (get-range [dbi-name k-range])
    (get-range [dbi-name k-range k-type])
    (get-range [dbi-name k-range k-type v-type])
    (get-range [dbi-name k-range k-type v-type ignore-key?])
    (get-rank [dbi-name k])
    (get-rank [dbi-name k k-type])
    (get-some [dbi-name pred k-range])
    (get-some [dbi-name pred k-range k-type])
    (get-some [dbi-name pred k-range k-type v-type])
    (get-some [dbi-name pred k-range k-type v-type ignore-key?])
    (get-some [dbi-name pred k-range k-type v-type ignore-key? raw-pred?])
    (get-value [dbi-name k])
    (get-value [dbi-name k k-type])
    (get-value [dbi-name k k-type v-type])
    (get-value [dbi-name k k-type v-type ignore-key?])
    (key-range [dbi-name k-range])
    (key-range [dbi-name k-range k-type])
    (key-range-count [dbi-name k-range])
    (key-range-count [dbi-name k-range k-type])
    (key-range-list-count [dbi-name k-range k-type])
    (range-count [dbi-name k-range])
    (range-count [dbi-name k-range k-type])
    (range-filter [dbi-name pred k-range])
    (range-filter [dbi-name pred k-range k-type])
    (range-filter [dbi-name pred k-range k-type v-type])
    (range-filter [dbi-name pred k-range k-type v-type ignore-key?])
    (range-filter [dbi-name pred k-range k-type v-type ignore-key? raw-pred?])
    (range-filter-count [dbi-name pred k-range])
    (range-filter-count [dbi-name pred k-range k-type])
    (range-filter-count [dbi-name pred k-range k-type v-type])
    (range-filter-count [dbi-name pred k-range k-type v-type raw-pred?])
    (range-keep [dbi-name pred k-range])
    (range-keep [dbi-name pred k-range k-type])
    (range-keep [dbi-name pred k-range k-type v-type])
    (range-keep [dbi-name pred k-range k-type v-type raw-pred?])
    (range-seq [dbi-name k-range])
    (range-seq [dbi-name k-range k-type])
    (range-seq [dbi-name k-range k-type v-type])
    (range-seq [dbi-name k-range k-type v-type ignore-key?])
    (range-seq [dbi-name k-range k-type v-type ignore-key? opts])
    (range-some [dbi-name pred k-range])
    (range-some [dbi-name pred k-range k-type])
    (range-some [dbi-name pred k-range k-type v-type])
    (range-some [dbi-name pred k-range k-type v-type raw-pred?])
    (sample-kv [dbi-name n])
    (sample-kv [dbi-name n k-type])
    (sample-kv [dbi-name n k-type v-type])
    (sample-kv [dbi-name n k-type v-type ignore-key?])
    (visit [dbi-name visitor k-range])
    (visit [dbi-name visitor k-range k-type])
    (visit [dbi-name visitor k-range k-type v-type])
    (visit [dbi-name visitor k-range k-type v-type raw-pred?])
    (visit-key-range [dbi-name visitor k-range])
    (visit-key-range [dbi-name visitor k-range k-type])
    (visit-key-range [dbi-name visitor k-range k-type raw-pred?])
    (visit-key-sample [dbi-name indices visitor k-range k-type])
    (visit-key-sample [dbi-name indices visitor k-range k-type raw-pred?])))

(defn raw-lmdb
  [db]
  (if (instance? KVLMDB db)
    (.-db ^KVLMDB db)
    db))

(defn read-value-result
  "A synchronous encoded point-read result for negotiated remote reads.
  Declared custom DBIs retain their logical codec and native receiver behavior."
  [db dbi-name k k-type v-type ignore-key?]
  (let [raw (raw-lmdb db)]
    (if (custom-kv/custom-dbi? raw dbi-name)
      (i/get-value db dbi-name k k-type v-type ignore-key?)
      (enc/read-result
        #(scan/write-value! raw dbi-name k k-type v-type ignore-key? %)))))

(defn read-range-result
  "An encoded range for negotiated remote reads below the copy-out threshold.
  Declared custom DBIs retain their logical codecs and ordinary result path."
  [db dbi-name k-range k-type v-type ignore-key?]
  (let [raw (raw-lmdb db)]
    (if (custom-kv/custom-dbi? raw dbi-name)
      (i/get-range db dbi-name k-range k-type v-type ignore-key?)
      (enc/read-result
        #(scan/write-range! raw dbi-name k-range k-type v-type ignore-key?
                            c/+wire-datom-batch-size+ %)))))

(deftype ^:no-doc ValueReaderState [raw dbis dbi custom?])

(defn value-reader
  "Compile fixed point-read arguments for local and server execution. A DBI
  handle is retained only while its environment and DBI metadata are current."
  [dbi-name k-type v-type ignore-key?]
  (let [state (AtomicReference.)
        decode (b/buffer-reader v-type)
        encode (enc/buffer-writer v-type)]
    (fn [db k encoded?]
      (let [raw (raw-lmdb db)
            _ (i/check-ready raw)
            info @(i/kv-info raw)
            dbis (:dbis info)
            ^ValueReaderState previous (.get state)
            ^ValueReaderState current
            (if (and previous (identical? raw (.-raw previous))
                     (identical? dbis (.-dbis previous)))
              previous
              (let [dbi (i/get-dbi raw dbi-name false)
                    current (ValueReaderState.
                              raw (:dbis @(i/kv-info raw)) dbi
                              (contains? (:custom-dbis @(i/kv-info raw)) dbi-name))]
                (.set state current)
                current))]
        (if (and (.-custom? current) (not l/*raw-kv?*))
          (i/get-value db dbi-name k k-type v-type ignore-key?)
          (if encoded?
            (enc/read-result
              #(scan/read-prepared-value raw (.-dbi current) k k-type
                                         decode encode ignore-key? %))
            (scan/read-prepared-value raw (.-dbi current) k k-type
                                     decode encode ignore-key? nil)))))))

(defn update-kv
  "Atomically replace the value at `k` with `(apply f old-value args)`.
  Missing keys pass nil to `f`. Returns :transacted. Only ordinary, single-value
  DBIs are supported. Remote functions must be serializable inter-fn functions.
  Compatibility mode can retry the function after map resize or an aborted
  commit group, so it must be free of side effects. Its concurrent standalone
  writes preserve Clojure dynamic bindings. Independent mode evaluates the
  function once on the batch owner and uses explicit request context."
  ([db dbi-name k f] (update-kv db dbi-name k f :data :data))
  ([db dbi-name k f k-type] (update-kv db dbi-name k f k-type :data))
  ([db dbi-name k f k-type v-type & args]
   (if (satisfies? i/IRemoteKV db)
     (i/remote-update-kv db dbi-name k f k-type v-type args)
     (grouped-write!
       db
       (fn [tx]
         (when (i/list-dbi? tx dbi-name)
           (raise "update-kv requires a single-value DBI" {:dbi-name dbi-name}))
         (let [value (apply f (i/get-value tx dbi-name k k-type v-type) args)]
           (i/transact-kv tx dbi-name [[:put k value]] k-type v-type)
           :transacted))))))

(defn prepare-get-value
  "Prepare a reusable KV point read. Execute it with a key using
  `execute-prepared` or by invoking the returned object. DBI changes refresh
  its metadata; transactions and buffers are acquired separately on each call."
  ([db dbi-name] (prepare-get-value db dbi-name :data :data true))
  ([db dbi-name k-type] (prepare-get-value db dbi-name k-type :data true))
  ([db dbi-name k-type v-type]
   (prepare-get-value db dbi-name k-type v-type true))
  ([db dbi-name k-type v-type ignore-key?]
   (if (satisfies? i/IRemotePrepared db)
     (i/prepare-remote-read db :get-value [dbi-name nil k-type v-type ignore-key?])
     (let [reader (value-reader dbi-name k-type v-type ignore-key?)]
       (i/check-ready (raw-lmdb db))
       (i/get-dbi (raw-lmdb db) dbi-name false)
       (prepared/prepared-read #(reader db % false)
                               #(reader %1 %2 false))))))

(defn wrap-lmdb
  [db]
  (if (instance? KVLMDB db)
    db
    (let [fallback-attempted?
          (true? (:txlog-recovery-fallback-attempted?
                  (i/env-opts db)))]
      (try
        (ensure-txlog-ready! db)
        (let [wrapped (->KVLMDB db nil)]
          (when-not (l/writing? db) (custom-kv/initialize! wrapped db))
          wrapped)
        (catch Exception e
          (if fallback-attempted?
            (do
              (close-failed-open! db)
              (throw e))
            (if-let [recovered (recover-from-snapshot-open! db e)]
              recovered
              (do
                (close-failed-open! db)
                (throw e)))))))))

(l/set-open-kv-wrapper! wrap-lmdb)

(kvtx/set-snapshot-scheduler-hooks! scheduler/start-snapshot-scheduler!
                                    scheduler/stop-snapshot-scheduler!)
