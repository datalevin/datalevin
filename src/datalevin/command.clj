;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.command
  "Wire command properties shared by HA admission and server dispatch.

  Every server handler must have an explicit entry in `properties`. Database
  writes require HA admission, commit fencing, and replica protection. Local
  administration is protected on read-only replicas but remains available on
  HA followers (for example, membership changes and retention floor reports).
  Session, store lifecycle, and read commands do not require these write gates.
  Transport replay safety is separate: only read-only commands or writes with
  supported client operation metadata may be resent after an ambiguous failure.")

(def ^:private unguarded
  {:ha-write? false :replica-write? false :read-only? true})

(def ^:private unguarded-write
  (assoc unguarded :read-only? false))

(def ^:private db-write
  {:ha-write? true :replica-write? true :read-only? false})

(def ^:private client-op-write
  ;; These handlers persist client operation IDs and their results atomically
  ;; with the mutation. Metadata on other commands does not enable deduplication.
  (assoc db-write :client-op? true))

(def ^:private local-write
  {:ha-write? false :replica-write? true :read-only? false})

(def ^:private index-open
  ;; The handler first attempts an existing-only open. Creation and legacy
  ;; migration run through write admission before touching persistent state.
  (assoc db-write :deferred-write? true :db-type "kv"))

(def ^:private search-read (assoc unguarded :db-type "engine"))
(def ^:private search-write (assoc db-write :db-type "engine"))
(def ^:private vector-read (assoc unguarded :db-type "index"))
(def ^:private vector-write (assoc db-write :db-type "index"))

(def ^:private transaction-open
  (assoc db-write :transaction :open))

(def ^:private transaction-close
  (assoc db-write :transaction :close))

(def ^:private transaction-abort
  ;; Aborts must remain available after demotion to release the transaction.
  (assoc db-write :transaction :abort))

(def ^:private runtime-managed
  ;; Open/close handlers take the runtime-store write lock themselves. Copy
  ;; holds a narrower read lock only during the snapshot, not file transfer.
  (assoc unguarded :runtime-read-access-exempt? true))

(def ^:private runtime-write
  (assoc runtime-managed :read-only? false))

(def properties
  "Command classifications. Keep entries in server handler-table order so
  additions can be reviewed together; tests require the tables to agree."
  {:authentication unguarded-write
   ;; Long polling must release the runtime-store lock while it waits.
   :db-changes (assoc runtime-managed :db-type "datalog")
   :disconnect unguarded-write
   :set-client-id unguarded-write
   :create-user unguarded-write
   :reset-password unguarded-write
   :drop-user unguarded-write
   :list-users unguarded
   :create-role unguarded-write
   :drop-role unguarded-write
   :list-roles unguarded
   :create-database unguarded-write
   :close-database runtime-write
   :drop-database local-write
   :list-databases unguarded
   :list-databases-in-use unguarded
   :assign-role unguarded-write
   :withdraw-role unguarded-write
   :list-user-roles unguarded
   :grant-permission unguarded-write
   :revoke-permission unguarded-write
   :list-role-permissions unguarded
   :list-user-permissions unguarded
   :query-system unguarded
   :show-clients unguarded
   :disconnect-client unguarded-write
   :open runtime-write
   :close unguarded-write
   :closed? unguarded
   :opts unguarded
   :assoc-opt local-write
   :assoc-opts local-write
   :last-modified unguarded
   :schema unguarded
   :rschema unguarded
   :set-schema db-write
   :datalog-register-type db-write
   :init-max-eid unguarded
   :max-tx unguarded
   :swap-attr db-write
   :del-attr db-write
   :rename-attr db-write
   :datom-count unguarded
   :load-datoms db-write
   :tx-data client-op-write
   :db-info unguarded
   :tx-data+db-info client-op-write
   :open-transact transaction-open
   :close-transact transaction-close
   :abort-transact transaction-abort
   :set-env-flags db-write
   :get-env-flags unguarded
   :sync local-write
   :ha-watermark unguarded
   :ha-update-membership! local-write
   :txlog-watermarks unguarded
   :open-tx-log unguarded
   :open-tx-log-rows unguarded
   :read-commit-marker unguarded
   :verify-commit-marker! unguarded
   :force-txlog-sync! local-write
   :force-lmdb-sync! local-write
   :create-snapshot! local-write
   :list-snapshots unguarded
   :snapshot-scheduler-state unguarded
   :txlog-retention-state unguarded
   :gc-txlog-segments! local-write
   :txlog-update-snapshot-floor! local-write
   :txlog-clear-snapshot-floor! local-write
   :txlog-update-replica-floor! local-write
   :txlog-clear-replica-floor! local-write
   :txlog-pin-backup-floor! local-write
   :txlog-unpin-backup-floor! local-write
   :replica-status unguarded
   :fetch unguarded
   :populated? unguarded
   :size unguarded
   :head unguarded
   :tail unguarded
   :slice unguarded
   :rslice unguarded
   :start-sampling unguarded-write
   :stop-sampling unguarded-write
   ;; Explicit analysis persists sampling metadata.
   :analyze db-write
   :e-datoms unguarded
   :e-first-datom unguarded
   :av-datoms unguarded
   :av-first-datom unguarded
   :av-first-e unguarded
   :ea-first-datom unguarded
   :ea-first-v unguarded
   :v-datoms unguarded
   :size-filter unguarded
   :head-filter unguarded
   :tail-filter unguarded
   :slice-filter unguarded
   :rslice-filter unguarded
   :open-kv runtime-write
   :close-kv unguarded-write
   :closed-kv? unguarded
   :open-dbi db-write
   :register-type db-write
   :clear-dbi db-write
   :drop-dbi db-write
   :list-dbis unguarded
   :copy runtime-managed
   :stat unguarded
   :entries unguarded
   :open-transact-kv transaction-open
   :close-transact-kv transaction-close
   :abort-transact-kv transaction-abort
   :transact-kv client-op-write
   :batch-kv unguarded
   :visit-key-range unguarded
   :get-some unguarded
   :range-filter unguarded
   :range-keep unguarded
   :range-some unguarded
   :range-filter-count unguarded
   :visit unguarded
   :visit-list unguarded
   :list-range-filter unguarded
   :list-range-some unguarded
   :list-range-keep unguarded
   :list-range-filter-count unguarded
   :visit-list-range unguarded
   :q unguarded
   :pull unguarded
   :pull-many unguarded
   :explain unguarded
   :fulltext-datoms unguarded
   :new-search-engine index-open
   :add-doc search-write
   :remove-doc search-write
   :clear-docs search-write
   :doc-indexed? search-read
   :doc-count search-read
   :search search-read
   :search-re-index search-write
   :new-vector-index index-open
   :add-vec vector-write
   :remove-vec vector-write
   :persist-vecs vector-write
   ;; Closing a vector index checkpoints it to LMDB.
   :close-vecs vector-write
   :clear-vecs vector-write
   :vecs-info vector-read
   :vec-indexed? vector-read
   :search-vec vector-read
   :vec-re-index vector-write
   :kv-re-index db-write
   :datalog-re-index db-write
   :get-value unguarded
   :get-rank unguarded
   :get-by-rank unguarded
   :sample-kv unguarded
   :get-first unguarded
   :get-first-n unguarded
   :get-range unguarded
   :key-range unguarded
   :key-range-count unguarded
   :key-range-list-count unguarded
   :range-count unguarded
   :get-list unguarded
   :list-count unguarded
   :in-list? unguarded
   :list-range unguarded
   :list-range-count unguarded
   :list-range-first unguarded
   :list-range-first-n unguarded})

(defn ha-write?
  "Whether a command requires HA write admission and commit guards."
  [type]
  (true? (:ha-write? (properties type))))

(defn replica-write?
  "Whether a command is forbidden on a read-only replica."
  [type]
  (true? (:replica-write? (properties type))))

(defn read-only?
  "Whether a command can be replayed after transport failure without deduplication.
  Unknown commands are not assumed to be safe."
  [type]
  (true? (:read-only? (properties type))))

(defn transaction-control
  "Return :open, :close, or :abort for transaction control commands."
  [type]
  (:transaction (properties type)))

(defn supports-client-op?
  "Whether the handler deduplicates writes carrying client operation metadata."
  [type]
  (true? (:client-op? (properties type))))

(defn deferred-write?
  "Whether the handler admits writes only when an existing-only open fails."
  [type]
  (true? (:deferred-write? (properties type))))

(defn db-type
  "Database/index type a retry endpoint must open for this command, if known."
  [type]
  (:db-type (properties type)))

(defn runtime-read-access-exempt?
  "Whether the handler manages its own runtime-store lock."
  [type]
  (true? (:runtime-read-access-exempt? (properties type))))
