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
           [datalevin.utl WriterContext]
           [clojure.lang Var]
           [java.util HashMap IdentityHashMap Set]
           [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *explicit-body* nil)

(deftype ^:private PreparationContext
  [flush! group ^:unsynchronized-mutable ^objects scalar-rows
   ^:unsynchronized-mutable ^boolean scalar-rows-in-use?
   ^LongObjectHashMap scalar-pending
   ^HashMap unique-pending
   ^:unsynchronized-mutable ^boolean scalar-pending?
   ^:unsynchronized-mutable ^long pending-start
   ^:unsynchronized-mutable ^long pending-indexed]
  txcommon/ScalarPendingIndex
  (pending-scalar-index [_] (when scalar-pending? scalar-pending))
  (pending-unique-index [_] (when scalar-pending? unique-pending))
  (retain-pending-index? [_] (not scalar-pending?))
  (prepare-pending-index! [_ tx-db]
    (when scalar-pending?
      ;; The native batch can admit more requests during execution. Populate
      ;; its hash view only when another simple request needs preceding writes.
      (let [^FastList datoms (.-datoms ^WriteGroup @group)
            n (.size datoms)]
        (loop [idx pending-indexed]
          (when (< idx n)
            (txcommon/stage-batch-datom! tx-db (.get datoms (int idx)))
            (recur (unchecked-inc idx))))
        (set! pending-indexed (long n)))))
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
      (.clear scalar-pending)
      (.clear unique-pending)))
  (discard-scalar-pending! [_]
    (.clear scalar-pending)
    (.clear unique-pending)
    ;; Every discard follows a native drain. The next segment can start with
    ;; the simple pending view even if its predecessor needed sorted indexes.
    (set! scalar-pending? (boolean true))
    (set! pending-start (long (.size ^FastList (.-datoms ^WriteGroup @group))))
    (set! pending-indexed pending-start))
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
    (.clear unique-pending)
    (db/-clear-tx-cache tx-db)
    (let [^FastList datoms (.-datoms ^WriteGroup @group)]
      (loop [idx pending-start]
        (when (< idx (.size datoms))
          (txcommon/stage-batch-datom! tx-db (.get datoms (int idx)))
          (recur (unchecked-inc idx)))))))

(defn control
  "Return the collector for a standalone local Datalog connection."
  [conn]
  (let [store (.-store ^DB @conn)]
    (when (instance? Store store)
      (let [lmdb (.-lmdb ^Store store)]
        (when-not (or (l/writing? lmdb)
                      (Thread/holdsLock (l/write-txn lmdb)))
          (let [control (:independent-control @(i/kv-info lmdb))]
            (when (and (:datalog-context control)
                       (not (and (:server? control) kv/*server-write-slot-held?*)))
              control)))))))

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
                              data (readable-result (:result data) after)))))
      committed)))

(defn- check-secondary-index-isolation! [b context]
  (when (or (not (:isolated? context)) (> (batch/batch-count b) 1))
    (batch/cancel-before-dispatch!
      (ex-info "Secondary indexing mode changed while write was queued"
               {:error :txlog/indexing-mode-changed
                :outcome :not-committed :retryable? true}))))

(defn- check-store-isolation! [b context store]
  (when (s/synchronous-secondary-indexing? store)
    (check-secondary-index-isolation! b context)))

(defn- check-isolation! [b descriptor]
  (let [context (batch/context descriptor)]
    (when-let [conn (:datalog-conn context)]
      ;; Admission is only a snapshot. Recheck queued requests before starting
      ;; the batch, and validate the writing Store again after its transfer.
      (check-store-isolation! b context (:store @conn)))))

(defn attach!
  "Attach the existing Datalog store to the environment's data collector."
  ([db] (attach! db nil))
  ([^DB db hooks]
    (when (instance? Store (.-store db))
      (let [raw (.-lmdb ^Store (.-store db))
            context (volatile! nil)]
        (embedded/attach!
          raw
          (merge hooks
            {:datalog? true :context context
             :wrap-execution
             (fn [execute b]
               (let [ctx {:current (volatile! nil) :group (volatile! nil)
                          :cache (volatile! nil) :connections (IdentityHashMap.)
                          :native-view (object-array 2)
                          :resolver-native (volatile! nil)
                          :resolver-store (volatile! nil)
                          :resolver-indexes (volatile! nil)
                          :resolver-bindings (volatile! nil)
                          :validate-secondary-indexing!
                          (fn [lmdb]
                            (check-secondary-index-isolation!
                              b (l/request-context (kv/raw-lmdb lmdb))))
                          :batch b}]
                 (vreset! context ctx)
                 (try (execute)
                      (finally
                        (when-let [[store disabled?] @(:cache ctx)]
                          (when-not disabled? (db/enable-cache store)))
                        (vreset! context nil)))))
             :native-view! (fn [native]
                             ;; This is the owned writer, not an exposed member
                             ;; handle. RMW refreshes it after native map growth.
                             (vreset! (:resolver-native @context) native))
             :finish-preparation! (fn [_ _]
                                    (when-let [group @(:group @context)]
                                      (s/write-group-metadata group)))
             :storage-rows! (fn [native]
                              (when-let [group @(:group @context)]
                                (binding [s/*write-group* group
                                          s/*enforce-blind-unique-inserts?* false
                                          c/*ordered-datom-writes?* false]
                                  (s/take-group-storage-rows! group native))))
             :storage-tail! (fn [native]
                              (when-let [group @(:group @context)]
                                (s/freeze-group-storage-datoms! group native)))
             :before-batch! (fn [b]
                              (dotimes [idx (batch/batch-count b)]
                                (check-isolation! b (batch/batch-at b idx))))
             :before-body! (fn [descriptor]
                             ;; Native preparation can collect more requests
                             ;; after the initial batch check.
                             (check-isolation! (:batch @context) descriptor)
                             (when-not (:datalog-prepare? (batch/context descriptor))
                               (when-let [current @(:current @context)]
                                 (db/-clear-tx-cache current))
                               (when-let [preparation
                                          (get @(:resolver-bindings @context)
                                               #'txcommon/*batch-prepare*)]
                                 (txcommon/discard-scalar-pending! preparation))))
             :committed! (fn [b]
                           (when-let [committed (publish! raw @context b)]
                             (when-let [publish-db! (:publish-db! hooks)]
                               (publish-db! committed))))}))))
    db))
(defn batched? [control]
  (> (batch/batch-count (:batch @(:datalog-context control))) 1))

(defn- resolver-indexes!
  [context store]
  (vreset! (:resolver-store context) store)
  (or @(:resolver-indexes context)
      (let [native (kv/->KVLMDB @(:resolver-native context) nil)
            index-store (s/transfer store native)
            store-ref (:resolver-store context)
            indexes (db/batch-tx-indexes
                      index-store #(i/schema @store-ref)
                      (fn []
                        (let [lmdb (.-lmdb ^Store @store-ref)]
                          ;; UDF resolvers may retain :kv or issue writes. Retire
                          ;; this member's reusable scope before exposing it,
                          ;; and keep all writes behind its row capture.
                          (when-let [flush! (l/prepare-flush (kv/raw-lmdb lmdb))]
                            (flush!))
                          lmdb)))]
        (vreset! (:resolver-indexes context) indexes)
        indexes)))

(defn- writing-db!
  [context conn lmdb]
  (let [^DB before @conn
        ^DB previous (or @(:current context) before)
        ^Store store (.-store previous)
        _ (when-not @(:current context) (s/maybe-ensure-current! store))
        tx-store (if (identical? (kv/raw-lmdb (.-lmdb store))
                                (kv/raw-lmdb lmdb))
                   store (s/transfer store lmdb))
        ;; Option publication can race the earlier queue checks. Transfer owns
        ;; the options snapshot this writer will use; validate that exact Store
        ;; before user code or secondary indexing can mutate external state.
        _ (check-store-isolation! (:batch context)
                                  (l/request-context (kv/raw-lmdb lmdb)) tx-store)
        tx-db (cond-> (cond
                       (:datalog-prepare? (l/request-context (kv/raw-lmdb lmdb)))
                       ;; Order caches belong to the batch; custom functions
                       ;; receive the active member's capture and validity.
                       (let [[eavt avet] (resolver-indexes! context tx-store)]
                         (if (and (identical? tx-store store)
                                  (identical? eavt (:eavt previous))
                                  (identical? avet (:avet previous)))
                           previous
                           (assoc previous :store tx-store :eavt eavt :avet avet)))

                       (and (identical? tx-store store)
                            (empty? (:eavt previous)) (empty? (:avet previous)))
                       previous

                       :else (db/transfer previous tx-store))
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
                          #'s/*validate-secondary-indexing!*
                          (:validate-secondary-indexing! context)
                          #'txcommon/*batch-prepare*
                          (PreparationContext. (l/prepare-flush native)
                                               (:group context) nil false
                                               (.-scalar-pending ^WriteGroup @(:group context))
                                               (HashMap.) true
                                               (long (.size ^FastList
                                                            (.-datoms ^WriteGroup
                                                                      @(:group context))))
                                               (long (.size ^FastList
                                                            (.-datoms ^WriteGroup
                                                                      @(:group context)))))}]
            (vreset! (:resolver-bindings context) bindings)
            bindings))
      {#'s/*write-group* @(:group context)
       #'s/*validate-secondary-indexing!* (:validate-secondary-indexing! context)
       #'txcommon/*batch-prepare* nil})))

(defn- native-kv-view! [context native]
  (let [^objects scope (:native-view context)]
    (when-not (identical? native (aget scope 0))
      (aset scope 0 native)
      (aset scope 1 (kv/->KVLMDB native nil)))
    (aget scope 1)))

(defn- prepare-explicit-request!
  [context conn tx-data tx-meta transact!]
  (let [before @conn
        [eavt avet] (resolver-indexes! context (:store before))
        tx-db (assoc before :eavt eavt :avet avet)]
    (reset! conn tx-db)
    (Var/pushThreadBindings (resolver-bindings! context tx-db))
    (try
      (txcommon/prepare-pending-index! txcommon/*batch-prepare* tx-db)
      (let [report (transact! conn tx-data tx-meta)
            after (assoc @conn :eavt (:eavt before) :avet (:avet before))]
        ;; Resolver tombstones are private. Public lookup refs must not
        ;; interpret a pending retraction as a live identity.
        (reset! conn after)
        (assoc report :db-before before :db-after after))
      (catch Throwable t
        ;; A caught validation/CAS failure must not leave partial resolver
        ;; state for the next inner request. Rebuild from accepted datoms.
        (try (txcommon/restore-preparation! txcommon/*batch-prepare* tx-db)
             (catch Throwable restore-error
               (when-not (identical? t restore-error)
                 (.addSuppressed t restore-error))))
        (reset! conn before)
        (throw t))
      (finally
        (Var/popThreadBindings)
        (swap! conn assoc :eavt (:eavt before) :avet (:avet before))))))

(defn run-explicit-request!
  "Share preparation once adjacent inner writes form a burst. User reads drain
  each segment; later bursts can reuse the same preparation context."
  [conn tx-data tx-meta transact!]
  (let [{:keys [context before-read! last-write native-read]} *explicit-body*
        native (kv/raw-lmdb (.-lmdb ^Store (:store @conn)))
        ^WriterContext writer (l/writer-context native)
        request-context (.requestContext writer)
        preparing? (some? @last-write)]
    (i/check-ready native)
    (when-not (identical? (Thread/currentThread) (.owner writer))
      (l/write-txn native))
    ;; Ownership checks borrow the writer too. Resolution handles its own
    ;; pending state, after any earlier eager native tail has been applied.
    (.setBeforeRead writer nil)
    (when-let [read! @native-read]
      (vreset! native-read nil)
      (read!))
    (when preparing?
      (.setRequestContext writer (assoc request-context :datalog-prepare? true)))
    (vreset! last-write :resolving)
    (try
      (let [report (if preparing?
                     (prepare-explicit-request! context conn tx-data tx-meta transact!)
                     (transact! conn tx-data tx-meta))]
        (vreset! last-write (if preparing? :prepared :eager))
        report)
      (catch Throwable t
        ;; Accepted writes before a rejected request still need a read drain.
        (vreset! last-write (when preparing? :prepared))
        (throw t))
      (finally
        (vreset! native-read (.beforeRead writer))
        (.setRequestContext writer request-context)
        (.setBeforeRead writer before-read!)))))

(defn- execute-explicit-body!
  [context tx body]
  (let [native (kv/raw-lmdb (.-lmdb ^Store (:store @tx)))
        ^WriterContext writer (l/writer-context native)
        last-write (volatile! nil)
        native-read (volatile! nil)
        before-read! (fn []
                       ;; Detach before draining: native application may grow
                       ;; the map or borrow this view again while applying rows.
                       (.setBeforeRead writer nil)
                       (let [prepared? (= :prepared @last-write)
                             read! @native-read]
                         ;; Reentrant callbacks during the drain use their
                         ;; existing native scope, just as request resolution.
                         (vreset! last-write :resolving)
                         (vreset! native-read nil)
                         (if prepared?
                           (when-let [flush! (l/prepare-flush native)] (flush!))
                           (when read! (read!))))
                       (when-let [preparation
                                  (get @(:resolver-bindings context)
                                       #'txcommon/*batch-prepare*)]
                         (txcommon/discard-scalar-pending! preparation))
                       (doseq [^Set index @(:resolver-indexes context)]
                         (.clear index))
                       ;; Keep the explicit body active and rearm its burst
                       ;; gate. A single write/read pair stays eager; a second
                       ;; consecutive write can share preparation again.
                       (vreset! last-write nil))]
    (binding [*explicit-body* {:connection tx :context context
                              :before-read! before-read! :last-write last-write
                              :native-read native-read}]
      (try (body tx)
           (finally (.setBeforeRead writer nil))))))

(defn- execute-bound-body!
  [context conn tx-db body publication notifications]
  (when-let [preparation (get @(:resolver-bindings context) #'txcommon/*batch-prepare*)]
    (if (:datalog-fast? (l/request-context
                        (kv/raw-lmdb (.-lmdb ^Store (:store tx-db)))))
      (txcommon/prepare-pending-index! preparation tx-db)
      (txcommon/materialize-scalar-pending! preparation tx-db)))
  (let [tx (atom tx-db :meta (cond-> (meta conn)
                              notifications (assoc ::notifications notifications)))
        result (if (:datalog-prepare?
                     (l/request-context (kv/raw-lmdb (.-lmdb ^Store (:store tx-db)))))
                 (body tx)
                 (execute-explicit-body! context tx body))]
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
      (when-let [preparation txcommon/*batch-prepare*]
        (txcommon/prepare-pending-index! preparation tx-db))
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
         (assoc opts :context (assoc (:context opts) :datalog-conn conn
                               :datalog-publication publication
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
