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
  Session, store lifecycle, and read commands do not require these write gates.")

(def ^:private unguarded
  {:ha-write? false :replica-write? false})

(def ^:private db-write
  {:ha-write? true :replica-write? true})

(def ^:private local-write
  {:ha-write? false :replica-write? true})

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

(def properties
  "Command classifications. Keep entries in server handler-table order so
  additions can be reviewed together; tests require the tables to agree."
  {:authentication unguarded
   :disconnect unguarded
   :set-client-id unguarded
   :create-user unguarded
   :reset-password unguarded
   :drop-user unguarded
   :list-users unguarded
   :create-role unguarded
   :drop-role unguarded
   :list-roles unguarded
   :create-database unguarded
   :close-database runtime-managed
   :drop-database local-write
   :list-databases unguarded
   :list-databases-in-use unguarded
   :assign-role unguarded
   :withdraw-role unguarded
   :list-user-roles unguarded
   :grant-permission unguarded
   :revoke-permission unguarded
   :list-role-permissions unguarded
   :list-user-permissions unguarded
   :query-system unguarded
   :show-clients unguarded
   :disconnect-client unguarded
   :open runtime-managed
   :close unguarded
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
   :tx-data db-write
   :db-info unguarded
   :tx-data+db-info db-write
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
   :start-sampling unguarded
   :stop-sampling unguarded
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
   :open-kv runtime-managed
   :close-kv unguarded
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
   :transact-kv db-write
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
   :new-search-engine unguarded
   :add-doc db-write
   :remove-doc db-write
   :clear-docs db-write
   :doc-indexed? unguarded
   :doc-count unguarded
   :search unguarded
   :search-re-index db-write
   :new-vector-index unguarded
   :add-vec db-write
   :remove-vec db-write
   :persist-vecs db-write
   ;; Closing a vector index checkpoints it to LMDB.
   :close-vecs db-write
   :clear-vecs db-write
   :vecs-info unguarded
   :vec-indexed? unguarded
   :search-vec unguarded
   :vec-re-index db-write
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

(defn transaction-control
  "Return :open, :close, or :abort for transaction control commands."
  [type]
  (:transaction (properties type)))

(defn runtime-read-access-exempt?
  "Whether the handler manages its own runtime-store lock."
  [type]
  (true? (:runtime-read-access-exempt? (properties type))))
