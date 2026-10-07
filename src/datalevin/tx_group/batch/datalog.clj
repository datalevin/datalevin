;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.datalog
  "Datalog bookkeeping around the shared native/WAL collector. Transaction
  reads use LMDB; connection values and caches are published after commit."
  (:require [datalevin.db :as db]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.db.tx.common :as txcommon]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.storage :as s]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.embedded :as embedded])
  (:import [datalevin.db DB TxReport]
           [datalevin.storage Store WriteGroup]
           [clojure.lang Var]
           [java.util IdentityHashMap]
           [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
           [org.eclipse.collections.impl.list.mutable FastList]))

(deftype ^:private PreparationContext
  [flush! group ^:unsynchronized-mutable ^objects scalar-rows
   ^:unsynchronized-mutable ^boolean scalar-rows-in-use?
   ^LongObjectHashMap scalar-pending
   ^:unsynchronized-mutable ^boolean scalar-pending?
   ^:unsynchronized-mutable ^long pending-start]
  txcommon/ScalarPendingIndex
  (pending-scalar-index [_] (when scalar-pending? scalar-pending))
  (materialize-scalar-pending! [_ tx-db]
    (when scalar-pending?
      ;; General resolution needs E/A/V tombstones and sorted indexes. Replay
      ;; the group's existing ordered datoms only at that boundary.
      (set! scalar-pending? (boolean false))
      (let [^FastList datoms (.-datoms ^WriteGroup @group)]
        (loop [idx pending-start]
          (when (< idx (.size datoms))
            (txcommon/stage-batch-datom! tx-db (.get datoms (int idx)))
            (recur (unchecked-inc idx)))))
      (.clear scalar-pending)))
  (discard-scalar-pending! [_]
    (.clear scalar-pending)
    (set! pending-start (long (.size ^FastList (.-datoms ^WriteGroup @group)))))
  txcommon/ScalarDatomBuffer
  (acquire-scalar-datom-buffer! [_ capacity]
    (when-not scalar-rows-in-use?
      (when (or (nil? scalar-rows) (< (alength scalar-rows) (long capacity)))
        (set! scalar-rows (object-array (int capacity))))
      (set! scalar-rows-in-use? (boolean true))
      scalar-rows))
  (release-scalar-datom-buffer! [_ buffer]
    (when (identical? buffer scalar-rows)
      (set! scalar-rows-in-use? (boolean false))))
  txcommon/BatchPreparation
  (flush-preparation! [_] (when flush! (flush!)))
  (restore-preparation! [_ tx-db]
    ;; Retry alone rebuilds this request's resolver indexes. The normal path
    ;; needs neither a restore closure nor a request-specific context map.
    (set! scalar-pending? (boolean false))
    (.clear scalar-pending)
    (db/-clear-tx-cache tx-db)
    (doseq [datom (.-datoms ^WriteGroup @group)]
      (txcommon/stage-batch-datom! tx-db datom))))

(defn control
  "Return the collector for a standalone local Datalog connection."
  [conn]
  (let [store (.-store ^DB @conn)]
    (when (instance? Store store)
      (let [lmdb (.-lmdb ^Store store)]
        (when-not (or (l/writing? lmdb)
                      (Thread/holdsLock (l/write-txn lmdb)))
          (let [control (:independent-control @(i/kv-info lmdb))]
            (when (:datalog-context control) control)))))))

(defn- local-writing-view?
  [view after]
  (and (instance? DB view)
       (instance? Store (:store view))
       (let [lmdb (.-lmdb ^Store (:store view))]
         (and (l/writing? lmdb)
              (identical? (.-write-txn ^Store (:store view))
                          (.-write-txn ^Store (:store after)))))))

(defn- readable-result
  [result after]
  (cond
    (instance? TxReport result)
    (if (local-writing-view? (:db-after result) after)
      (assoc result :db-before (if (local-writing-view? (:db-before result) after)
                                (db/transfer-report-db (:db-before result) after)
                                (:db-before result))
                    :db-after after)
      result)
    (instance? DB result) (if (local-writing-view? result after)
                           (db/transfer-report-db result after) result)
    (vector? result) (mapv #(readable-result % after) result)
    (map? result) (reduce-kv #(assoc %1 %2 (readable-result %3 after)) result result)
    (list? result) (apply list (map #(readable-result % after) result))
    :else result))

(defn- publish!
  [raw context b]
  (when-let [^DB current @(:current context)]
    (let [^Store store (.-store current)
          ^IdentityHashMap connections (:connections context)
          ;; Schema mutations on a writing Store defer auxiliary index setup.
          changed? (some #(not (identical? (i/schema (:store (:before %)))
                                          (i/schema store)))
                         (.values connections))
          committed-store (if changed?
                            (s/transfer-after-schema-change store raw)
                            (s/transfer store raw))
          committed (db/transfer current committed-store)
          ^WriteGroup group @(:group context)]
      (db/invalidate-write-group! committed-store group)
      (doseq [entry (.entrySet connections)]
        (let [conn (.getKey ^java.util.Map$Entry entry)
              {:keys [before publications]} (.getValue ^java.util.Map$Entry entry)
              after (db/carry-runtime-opts committed before)]
          (reset! conn after)
          (doseq [publication publications] (vreset! publication after))))
      (db/adopt-current-db! committed)
      (dotimes [idx (batch/batch-count b)]
        (let [d (batch/batch-at b idx)
              data (batch/data d)
              publication (:datalog-publication (batch/context d))
              conn (:datalog-conn (batch/context d))
              after (or (when conn @conn)
                        (when publication @publication) committed)]
          (batch/set-data! d (batch/with-data-result!
                              data (readable-result (:result data) after))))))))

(defn attach!
  "Attach the existing Datalog store to the environment's data collector."
  [^DB db]
  (when (instance? Store (.-store db))
    (let [raw (.-lmdb ^Store (.-store db))
          context (volatile! nil)]
      (embedded/attach!
        raw
        {:datalog? true :context context
         :wrap-execution
         (fn [execute b]
           (let [ctx {:current (volatile! nil) :group (volatile! nil)
                      :cache (volatile! nil) :connections (IdentityHashMap.)
                      :native-view (object-array 2)
                      :resolver-bindings (volatile! nil)
                      :batch b}]
             (vreset! context ctx)
             (try (execute)
                  (finally
                    (when-let [[store disabled?] @(:cache ctx)]
                      (when-not disabled? (db/enable-cache store)))
                    (vreset! context nil)))))
         :finish-preparation! (fn [_ _]
                                (when-let [group @(:group @context)]
                                  (s/write-group-metadata group)))
         :storage-rows! (fn []
                          (when-let [group @(:group @context)]
                            (binding [s/*write-group* group
                                      s/*enforce-blind-unique-inserts?* false
                                      c/*ordered-datom-writes?* false]
                              (s/take-group-storage-rows! group))))
         :storage-tail! (fn []
                          (when-let [group @(:group @context)]
                            (s/freeze-group-storage-datoms! group)))
         :before-body! (fn [descriptor]
                         (when-not (:datalog-prepare? (batch/context descriptor))
                           (when-let [current @(:current @context)]
                             (db/-clear-tx-cache current))
                           (when-let [preparation
                                      (get @(:resolver-bindings @context)
                                           #'txcommon/*batch-prepare*)]
                             (txcommon/discard-scalar-pending! preparation))))
         :committed! #(publish! raw @context %)})))
  db)

(defn batched? [control]
  (> (batch/batch-count (:batch @(:datalog-context control))) 1))

(defn- writing-db!
  [context conn lmdb]
  (let [^DB before @conn
        ^DB previous (or @(:current context) before)
        ^Store store (.-store previous)
        _ (when-not @(:current context) (s/maybe-ensure-current! store))
        tx-store (if (identical? (kv/raw-lmdb (.-lmdb store))
                                (kv/raw-lmdb lmdb))
                   store (s/transfer store lmdb))
        tx-db (cond-> (if (and (identical? tx-store store)
                              (or (:datalog-prepare? (l/request-context (kv/raw-lmdb lmdb)))
                                  (and (empty? (:eavt previous))
                                       (empty? (:avet previous)))))
                       previous (db/transfer previous tx-store))
                (nil? @(:current context))
                (assoc :max-eid (i/init-max-eid tx-store)
                       :max-tx (i/max-tx tx-store)))
        tx-db (if (identical? (db/runtime-opts tx-db) (db/runtime-opts before))
                tx-db (db/carry-runtime-opts tx-db before))]
    (when-not @(:cache context)
      (vreset! (:cache context) [store (db/cache-disabled? store)])
      (db/disable-cache store))
    (when-not @(:group context)
      (vreset! (:group context) (s/write-group lmdb)))
    tx-db))

(defn- record-connection!
  [context conn publication]
  (let [^IdentityHashMap connections (:connections context)
        entry (or (.get connections conn) {:before @conn})]
    (.put connections conn (if publication
                             (update entry :publications (fnil conj []) publication)
                             entry))))

(defn- resolver-bindings! [context tx-db]
  (let [native (kv/raw-lmdb (.-lmdb ^Store (:store tx-db)))]
    (if (:datalog-prepare? (l/request-context native))
      (or @(:resolver-bindings context)
          (let [bindings {#'s/*write-group* @(:group context)
                          #'txcommon/*batch-prepare*
                          (PreparationContext. (:native-prepare-flush! (meta native))
                                               (:group context) nil false
                                               (.-scalar-pending ^WriteGroup @(:group context))
                                               true
                                               (long (.size ^FastList
                                                            (.-datoms ^WriteGroup
                                                                      @(:group context)))))}]
            (vreset! (:resolver-bindings context) bindings)
            bindings))
      {#'s/*write-group* @(:group context) #'txcommon/*batch-prepare* nil})))

(defn- native-kv-view! [context native]
  (let [^objects scope (:native-view context)]
    (when-not (identical? native (aget scope 0))
      (aset scope 0 native)
      (aset scope 1 (kv/->KVLMDB native nil)))
    (aget scope 1)))

(defn- execute-bound-body!
  [context conn tx-db body publication notifications]
  (when-let [preparation (get @(:resolver-bindings context) #'txcommon/*batch-prepare*)]
    (txcommon/materialize-scalar-pending! preparation tx-db))
  (let [tx (atom tx-db :meta (cond-> (meta conn)
                              notifications (assoc ::notifications notifications)))
        result (body tx)]
    (record-connection! context conn publication)
    (vreset! (:current context) @tx)
    result))

(defn- execute-body!
  [context conn tx-db body publication notifications]
  (Var/pushThreadBindings (resolver-bindings! context tx-db))
  (try (execute-bound-body! context conn tx-db body publication notifications)
       (finally (Var/popThreadBindings))))

(defn- execute-scalar!
  [control conn prepared tx-data tx-meta stamp! fallback! native]
  (let [context @(:datalog-context control)
        tx-db (writing-db! context conn (native-kv-view! context native))]
    (Var/pushThreadBindings (resolver-bindings! context tx-db))
    (try
      (if-let [report (stamp! control tx-db prepared tx-meta)]
        (do (record-connection! context conn nil)
            (vreset! (:current context) (:db-after report))
            report)
        ;; Stale preparation uses the general runner in this same transaction.
        ;; No caller body is resubmitted or evaluated twice.
        (execute-bound-body! context conn tx-db
                             #(fallback! % tx-data tx-meta) nil nil))
      (finally (Var/popThreadBindings)))))

(defn run-scalar!
  "Stamp a prepared scalar request against the current native batch DB.
  Avoid transaction-local connection/publication/listener wrappers on success."
  [control conn prepared tx-data tx-meta stamp! fallback!]
  (let [submitter (Thread/currentThread)
        ;; Submission blocks until execution completes, so the caller's frame
        ;; remains live. Push/pop creates new frames; retain this one without
        ;; cloning and restore the owner after cross-thread execution.
        frame (Var/getThreadBindingFrame)]
    ((:internal-body! control)
     (fn [native]
       (if (identical? submitter (Thread/currentThread))
         (execute-scalar! control conn prepared tx-data tx-meta stamp! fallback! native)
         (let [owner-frame (Var/getThreadBindingFrame)]
           (Var/resetThreadBindingFrame frame)
           (try (execute-scalar! control conn prepared tx-data tx-meta stamp! fallback! native)
                (finally (Var/resetThreadBindingFrame owner-frame))))))
     {:datalog-conn conn :datalog-prepare? true
      :isolated? (s/synchronous-secondary-indexing? (:store @conn))})))

(defn with-transaction!
  "Run one Datalog body on the elected native owner and publish after commit."
  [control conn body opts]
  (let [publication (volatile! nil)
        notifications (FastList.)
        result
        ((:body! control)
         (fn [lmdb]
           (let [context @(:datalog-context control)]
             (execute-body! context conn (writing-db! context conn lmdb)
                            body publication notifications)))
         (assoc opts :context (assoc (:context opts) :datalog-publication publication
                               :isolated? (s/synchronous-secondary-indexing?
                                            (:store @conn)))))]
    ;; Explicit abort returns the body's result but publishes neither its
    ;; staged connection nor its listeners. Callbacks run after owner handoff.
    (when-let [after @publication]
      (doseq [[callback report] notifications]
        (callback (readable-result report after))))
    result))

(defn defer-listeners!
  "Retain transaction-local listeners until the enclosing native commit."
  [conn report]
  (when-let [^FastList notifications (::notifications (meta conn))]
    (doseq [[_ callback] (some-> (:listeners (meta conn)) deref)]
      (.add notifications [callback report]))
    true))
