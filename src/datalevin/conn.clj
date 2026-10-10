;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.conn
  "Datalog DB connection"
  (:require
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.db.tx.common :as txcommon]
   [datalevin.lmdb :as l]
   [datalevin.storage :as s]
   [datalevin.async :as a]
   [datalevin.remote :as r]
   [datalevin.util :as u :refer [raise]]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.tx-group.batch.datalog :as datalog]
   [datalevin.tx-group.compat :as group]
   [datalevin.validate :as vld])
  (:import
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.remote DatalogStore]
   [datalevin.async IAsyncWork IBoundedAsyncWork AsyncExecutor]
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util.concurrent Executors LinkedBlockingQueue ConcurrentHashMap
    ThreadPoolExecutor ArrayBlockingQueue ThreadPoolExecutor$CallerRunsPolicy
    TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean]))

(declare close closed? remove-conn shutdown-transact-async-executor!
         shutdown-transact-async-executor-if-idle!)

(defonce ^:private shared-local-stores (atom {}))

(defn conn?
  [conn]
  (and (instance? clojure.lang.IDeref conn) (db/db? @conn)))

(defn datalog-kv
  "Return the KV handle backing a Datalog connection or DB."
  [x]
  (let [db    (cond
                (conn? x) @x
                (db/db? x) x
                :else
                (raise "Expected a Datalog connection or DB"
                         {:input x}))
        store (.-store ^DB db)]
    (cond
      (instance? Store store)
      (.-lmdb ^Store store)

      (instance? DatalogStore store)
      (r/datalog-kv store)

      :else
      (raise "Datalog DB does not expose a KV handle"
               {:store store}))))

(deftype ^:private CloseableConn [^clojure.lang.Atom state]
  clojure.lang.IDeref
  (deref [_]
    @state)

  clojure.lang.IAtom2
  (swap [_ f]
    (swap! state f))
  (swap [_ f x]
    (swap! state f x))
  (swap [_ f x y]
    (swap! state f x y))
  (swap [_ f x y args]
    (apply swap! state f x y args))
  (swapVals [_ f]
    (swap-vals! state f))
  (swapVals [_ f x]
    (swap-vals! state f x))
  (swapVals [_ f x y]
    (swap-vals! state f x y))
  (swapVals [_ f x y args]
    (apply swap-vals! state f x y args))
  (compareAndSet [_ oldv newv]
    (compare-and-set! state oldv newv))
  (reset [_ newv]
    (reset! state newv))
  (resetVals [_ newv]
    (reset-vals! state newv))

  clojure.lang.IMeta
  (meta [_]
    (meta state))

  java.io.Closeable
  (close [this]
    (datalevin.conn/close this)))

(defn- conn-state
  [conn]
  (if (instance? CloseableConn conn)
    (.-state ^CloseableConn conn)
    conn))

(defn- wrap-conn
  [state]
  (CloseableConn. state))

(defn- alter-conn-meta!
  [conn f & args]
  (apply alter-meta! (conn-state conn) f args))

(defn conn-from-db
  [db]
  {:pre [(db/db? db)]}
  (datalog/attach! db)
  (wrap-conn
   (atom db :meta {:listeners (atom {})
                   :db-listeners (atom {})
                   :runtime-opts (db/runtime-opts db)})))

(defn conn-from-datoms
  ([datoms] (conn-from-db (db/init-db datoms)))
  ([datoms dir] (conn-from-db (db/init-db datoms dir)))
  ([datoms dir schema] (conn-from-db (db/init-db datoms dir schema)))
  ([datoms dir schema opts] (conn-from-db (db/init-db datoms dir schema opts))))

(defn- split-runtime-opts
  [opts]
  (if (map? opts)
    [(dissoc opts :runtime-opts) (:runtime-opts opts)]
    [opts nil]))

(defn- shared-local-store-key
  [dir]
  (when (and (string? dir) (not (u/dtlv-uri? dir)))
    (.getCanonicalPath ^java.io.File (u/file dir))))

(defn- acquire-shared-local-store
  [dir schema store-opts]
  (if-let [dir-key (shared-local-store-key dir)]
    (locking shared-local-stores
      (loop []
        (if-let [{:keys [store]} (get @shared-local-stores dir-key)]
          (if (i/closed? store)
            (do
              (swap! shared-local-stores dissoc dir-key)
              (recur))
            (do
              (swap! shared-local-stores update-in [dir-key :refs] inc)
              (when schema
                (i/set-schema store schema))
              store))
          (let [store (s/open dir schema store-opts)]
            (swap! shared-local-stores
                   assoc dir-key {:store store :refs 1})
            store))))
    (s/open dir schema store-opts)))

(defn- release-shared-local-store!
  [store]
  (if-let [dir-key (some-> (i/dir store) shared-local-store-key)]
    (locking shared-local-stores
      (if-let [{shared-store :store refs :refs}
               (get @shared-local-stores dir-key)]
        (if (i/closed? shared-store)
          (do
            (swap! shared-local-stores dissoc dir-key)
            :close)
          (if (identical? shared-store store)
            (if (> ^long refs 1)
              (do
                (swap! shared-local-stores update-in [dir-key :refs] dec)
                :detached)
              (do
                (swap! shared-local-stores dissoc dir-key)
                :close))
            :close))
        :close))
    :close))

(defn- open-conn-db
  [dir schema opts]
  {:pre [(or (nil? schema) (map? schema))]}
  (vld/validate-schema-update schema)
  (let [[_ runtime-opts] (split-runtime-opts opts)]
    (if (shared-local-store-key dir)
      (let [store (acquire-shared-local-store dir schema opts)]
        (cond-> (db/new-db store)
          (some? runtime-opts) (db/with-runtime-opts runtime-opts)))
      (db/empty-db dir schema opts))))

(defn create-conn
  ([] (conn-from-db (db/empty-db)))
  ([dir] (conn-from-db (open-conn-db dir nil nil)))
  ([dir schema] (conn-from-db (open-conn-db dir schema nil)))
  ([dir schema opts] (conn-from-db (open-conn-db dir schema opts))))

(defn close
  [conn]
  (when conn
    (let [detached? (volatile! false)]
    (try
      (when-let [listeners (:db-listeners (meta conn))]
        (doseq [[_ stop!] (first (reset-vals! listeners nil))]
          (stop!)))
      (when-not (closed? conn)
        (when-let [store (.-store ^DB @conn)]
          (case (release-shared-local-store! store)
            :detached
            (vreset! detached? true)

            :close
            (do
              (i/close store)
              (when (i/closed? store)
                (vreset! detached? true))))))
      (finally
        (when (or @detached? (closed? conn))
          (remove-conn (:dir (meta conn)) conn)
          (when-let [listeners (:listeners (meta conn))]
            (when (instance? clojure.lang.IAtom listeners)
              (reset! listeners {})))
          (when (instance? clojure.lang.IAtom conn)
            (reset! conn nil))
          (alter-conn-meta! conn dissoc :dir :remote-store-opts-cache))
        (shutdown-transact-async-executor-if-idle!)))))
  nil)

(defn closed?
  [conn]
  (or (nil? conn)
      (nil? @conn)
      (i/closed? ^Store (.-store ^DB @conn))))

(defn- active-conn-structural?
  "Cheap transaction-path check that avoids remote cache refreshes.

  `conn?` may call `last-modified` on remote stores to refresh caches, which
  adds an extra round-trip and can stall writes while HA leadership is
  converging. Transaction internals only need to know that the connection
  still derefs to a DB value."
  [conn]
  (and (instance? clojure.lang.IDeref conn)
       (let [db @conn]
         (instance? DB db))))

(defn abort-open-datalog-transaction!
  [store primary]
  (try
    (i/abort-transact store)
    (catch Throwable abort-error
      (.addSuppressed ^Throwable primary abort-error))))

(defmacro ^:no-doc with-compatibility-transaction
  "Retain the existing transaction implementation for unmigrated callers."
  [[conn orig-conn opts] & body]
  `(let [orig-conn#        ~orig-conn
         tx-timeout-opt#   (l/explicit-transaction-timeout-option ~opts)
         tx-timeout-ms#    (l/explicit-transaction-timeout-ms-from-option
                            tx-timeout-opt#)]
     (locking orig-conn#
       (let [db#  ^DB (deref orig-conn#)
             s#   (.-store db#)
             old# (db/cache-disabled? s#)]
         (db/disable-cache s#)
         (try
           (if (instance? DatalogStore s#)
             (locking (l/write-txn s#)
               (let [res#    (if (l/writing? s#)
                               (let [watchdog# (volatile! nil)]
                                 (try
                                   (vreset!
                                    watchdog#
                                    (l/start-explicit-transaction-watchdog!
                                     tx-timeout-ms#))
                                   (let [res# (let [~conn orig-conn#]
                                                ~@body)]
                                     (l/cancel-explicit-transaction-watchdog!
                                      @watchdog#)
                                     (l/assert-explicit-transaction-live!
                                      @watchdog#)
                                     res#)
                                   (catch Throwable t#
                                     (l/throw-explicit-transaction-failure!
                                      @watchdog# t#))
                                   (finally
                                     (l/cancel-explicit-transaction-watchdog!
                                      @watchdog#))))
                               (let [s1# (i/open-transact s#)
                                     w#  #(let [~conn
                                                (atom (db/transfer db# s1#)
                                                      :meta (meta orig-conn#))]
                                            ~@body)
                                     watchdog# (volatile! nil)]
                                 (try
                                   (vreset!
                                    watchdog#
                                    (l/start-explicit-transaction-watchdog!
                                     tx-timeout-ms#))
                                   (let [res# (u/repeat-try-catch
                                               ~c/+in-tx-overflow-times+
                                               l/resized? (w#))]
                                     (l/cancel-explicit-transaction-watchdog!
                                      @watchdog#)
                                     (l/assert-explicit-transaction-live!
                                      @watchdog#)
                                     (i/close-transact s#)
                                     res#)
                                   (catch Throwable t#
                                     (abort-open-datalog-transaction! s# t#)
                                     (l/throw-explicit-transaction-failure!
                                      @watchdog# t#))
                                   (finally
                                     (l/cancel-explicit-transaction-watchdog!
                                      @watchdog#)))))
                     new-db# (db/carry-runtime-opts (db/new-db s# nil db#) db#)]
                 (reset! orig-conn# new-db#)
                 res#))
             (let [kv#     (.-lmdb ^Store s#)
                   s1#     (volatile! nil)
                   res1#   (l/with-transaction-kv [kv1# kv# tx-timeout-opt#]
                             (let [conn1# (atom (db/transfer
                                                  db# (s/transfer s# kv1#))
                                                :meta (meta orig-conn#))
                                   res#   (let [~conn conn1#]
                                            ~@body)]
                               (vreset! s1# (.-store ^DB (deref conn1#)))
                               res#))
                   ;; A schema mutation may have been rolled back explicitly.
                   ;; Only adopt the schema that survived the transaction.
                   new-s#  (if (identical? (i/schema s#) (i/schema (deref s1#)))
                             (s/transfer (deref s1#) kv#)
                             (s/transfer-after-schema-change (deref s1#) kv#))
                   new-db# (db/carry-runtime-opts (db/new-db new-s# nil db#) db#)]
               (reset! orig-conn# new-db#)
               res1#))
           (finally
             (when-not old#
               (db/enable-cache (.-store ^DB (deref orig-conn#))))))))))

(defmacro with-transaction
  "Evaluate body within the context of a single new read/write transaction,
  ensuring atomicity of Datalog database operations. Works with synchronous
  `transact!`.

  `conn` is a new identifier of the Datalog database connection with a new
  read/write transaction attached, and `orig-conn` is the original database
  connection.

  `body` should refer to `conn`.

  The binding vector can include an optional opts map. `:timeout-ms` sets a
  timeout for the user body in milliseconds. Nil disables the timeout. A timeout
  interrupts the transaction thread and aborts the transaction when control
  returns to the macro; non-interruptible user code can still run until it
  returns.

  Example:

          (with-transaction [cn conn]
            (let [query  '[:find ?c .
                           :in $ ?e
                           :where [?e :counter ?c]]
                  ^long now (q query @cn 1)]
              (transact! cn [{:db/id 1 :counter (inc now)}])
              (q query @cn 1))) "
  [[conn orig-conn opts] & body]
  `(let [orig-conn# ~orig-conn
         opts# ~opts]
     (if-let [control# (datalog/control orig-conn#)]
       (datalog/with-transaction! control# orig-conn# (fn [~conn] ~@body) opts#)
       (with-compatibility-transaction [~conn orig-conn# opts#] ~@body))))

(defn with
  ([db tx-data] (with db tx-data {} false))
  ([db tx-data tx-meta] (with db tx-data tx-meta false))
  ([db tx-data tx-meta simulated?]
   (db/transact-tx-data (db/->TxReport db db [] {} tx-meta)
                        tx-data simulated?)))

(defn db-with
  [db tx-data]
  (:db-after (with db tx-data)))

(defn- with-isolated-tx-cache
  "Prepare against fresh mutable EAV/AVE overlays so readers of the currently
  published DB value cannot observe transaction-local mutations."
  [^DB db tx-data tx-meta simulated?]
  (db/transact-tx-data
    (db/->TxReport db (db/transfer db (.-store db)) [] {} tx-meta)
    tx-data simulated?))

(defn- local-direct-transact-eligible?
  [conn]
  (let [store (.-store ^DB @conn)]
    (and (instance? Store store)
         (let [lmdb (.-lmdb ^Store store)]
           ;; In WAL mode, route through with-transaction-kv so transaction
           ;; boundaries are explicitly anchored to LMDB write transactions.
           (and (not (true? (:wal? (l/read-env-opts lmdb))))
                (not (l/writing? lmdb)))))))

(defn- direct-local-transact!
  [conn tx-data tx-meta]
  (locking conn
    (let [db    ^DB (deref conn)
          store (.-store db)
          old   (db/cache-disabled? store)]
      (db/disable-cache store)
      (try
        (let [report (u/repeat-try-catch
                       c/+in-tx-overflow-times+
                       l/resized?
                       (with-isolated-tx-cache db tx-data tx-meta false))]
          (reset! conn (db/carry-runtime-opts (:db-after report) db))
          (assoc report :db-after @conn))
          (finally
            (when-not old
              (db/enable-cache (.-store ^DB @conn))))))))

(defn- direct-local-blind-transact!
  [conn prepared tx-meta]
  (locking conn
    (let [db    ^DB (deref conn)
          store ^Store (.-store db)]
      (when (db/blind-local-tx-valid? db prepared)
        (let [old (db/cache-disabled? store)]
          (db/disable-cache store)
          (try
            (let [kv             (.-lmdb store)
                  prepared-store (volatile! nil)
                  result
                  (l/with-transaction-kv [kv1 kv]
                    (let [store1 ^Store (s/transfer store kv1)
                          db1    ^DB    (db/transfer db store1)
                          fused? (boolean (:fuse-unique-inserts? prepared))]
                      (when (or fused?
                                (db/blind-local-tx-unique-values-absent?
                                  db1 prepared))
                        (vreset! prepared-store store1)
                        (let [report (db/stamp-blind-local-tx
                                       db1 prepared tx-meta)]
                          (binding [s/*enforce-blind-unique-inserts?* fused?
                                    c/*ordered-datom-writes?* true]
                            (db/commit-prepared-tx-data!
                              db1 (:tx-data report) report))
                          report))))]
              (when result
                (let [report result
                      new-store   ^Store (s/transfer
                                           ^Store @prepared-store kv)
                      new-db      (-> (:db-after report)
                                      (db/transfer new-store)
                                      (db/carry-runtime-opts db)
                                      (db/adopt-current-db!))]
                  (reset! conn new-db)
                  (assoc report :db-after @conn))))
            (catch clojure.lang.ExceptionInfo e
              (if (l/blind-unique-collision? e)
                nil
                (throw e)))
            (finally
              (when-not old
                (db/enable-cache (.-store ^DB @conn))))))))))

(defn- direct-local-identity-transact!
  [conn prepared tx-meta]
  (locking conn
    (let [db    ^DB @conn
          store ^Store (.-store db)]
      (when (db/blind-local-tx-valid? db prepared)
        (let [old (db/cache-disabled? store)]
          (db/disable-cache store)
          (try
            (let [kv             (.-lmdb store)
                  prepared-store (volatile! nil)
                  result
                  (l/with-transaction-kv [kv1 kv]
                    (let [store1 ^Store (s/transfer store kv1)
                          db1    ^DB    (db/transfer db store1)]
                      (when-let [[report upsert? :as result]
                                 (db/stamp-blind-local-identity-tx
                                   db1 prepared tx-meta)]
                        (vreset! prepared-store store1)
                        (binding [s/*enforce-blind-unique-inserts?* false
                                  c/*ordered-datom-writes?* (not upsert?)]
                          (db/commit-prepared-tx-data!
                            db1 (:tx-data report) report))
                        result)))]
              (when result
                (let [[report upsert?] result
                      new-store ^Store (s/transfer
                                         ^Store @prepared-store kv)
                      new-db    (-> (:db-after report)
                                    (db/transfer new-store)
                                    (db/carry-runtime-opts db)
                                    (db/adopt-current-db!))
                      report    (assoc report :db-after new-db)]
                  (reset! conn new-db)
                  [report upsert?])))
            (finally
              (when-not old
                (db/enable-cache (.-store ^DB @conn))))))))))

(defn- direct-local-patch-idoc-transact!
  [conn prepared tx-meta]
  (locking conn
    (let [db    ^DB @conn
          store ^Store (.-store db)]
      (when (db/local-patch-idoc-tx-valid? db prepared)
        (let [old (db/cache-disabled? store)]
          (db/disable-cache store)
          (try
            (let [kv             (.-lmdb store)
                  prepared-store (volatile! nil)
                  result
                  (l/with-transaction-kv [kv1 kv]
                    (let [store1 ^Store (s/transfer store kv1)
                          db1    ^DB    (db/transfer db store1)]
                      (when-let [{:keys [report]}
                                 (db/stamp-local-patch-idoc-tx
                                   db1 prepared tx-meta)]
                        (vreset! prepared-store store1)
                        (db/commit-prepared-tx-data!
                          db1 (:tx-data report) report)
                        report)))]
              (when result
                (let [report    result
                      new-store ^Store (s/transfer
                                         ^Store @prepared-store kv)
                      new-db    (-> (:db-after report)
                                    (db/transfer new-store)
                                    (db/carry-runtime-opts db)
                                    (db/adopt-current-db!))
                      report    (assoc report :db-after new-db)]
                  (reset! conn new-db)
                  report)))
            (finally
              (when-not old
                (db/enable-cache (.-store ^DB @conn))))))))))

(defn- maybe-direct-local-blind-transact!
  ([conn tx-data tx-meta]
   (maybe-direct-local-blind-transact! conn tx-data tx-meta false))
  ([conn tx-data tx-meta require-unique?]
   (when-let [prepared (db/prepare-blind-local-tx
                         ^DB @conn tx-data require-unique? false)]
     (when (or (not require-unique?)
               (:has-unique? prepared))
       (direct-local-blind-transact! conn prepared tx-meta)))))

(def ^:dynamic *local-wal-tx-path-observer* nil)
(def ^:dynamic *local-wal-identity-upsert?* true)
(def ^:dynamic *local-wal-patch-idoc?* true)

(defn- observe-local-wal-tx-path!
  [path]
  (when (fn? *local-wal-tx-path-observer*)
    (*local-wal-tx-path-observer* path)))

(defn- direct-local-scalar-update!
  "Commit a prepared simple scalar-update batch directly against one LMDB write
  transaction, without mutable index overlays or per-form report updates."
  [conn prepared tx-meta]
  (locking conn
    (let [db    ^DB (deref conn)
          store ^Store (.-store db)]
      (when (db/scalar-update-tx-valid? db prepared)
        (let [old (db/cache-disabled? store)]
          (db/disable-cache store)
          (try
            (let [kv             (.-lmdb store)
                  prepared-store (volatile! nil)
                  result
                  (l/with-transaction-kv [kv1 kv]
                    (let [store1 ^Store (s/transfer store kv1)
                          db1    ^DB    (db/transfer db store1)]
                      (when-let [report (db/stamp-scalar-update-tx
                                          db1 prepared tx-meta)]
                        (vreset! prepared-store store1)
                        (db/commit-prepared-tx-data!
                          db1 (:tx-data report) report)
                        report)))]
              (when result
                (let [report    result
                      new-store ^Store (s/transfer ^Store @prepared-store kv)
                      new-db    (-> (:db-after report)
                                    (db/transfer new-store)
                                    (db/carry-runtime-opts db)
                                    (db/adopt-current-db!))]
                  (reset! conn new-db)
                  (assoc report :db-after @conn))))
            (finally
              (when-not old
                (db/enable-cache (.-store ^DB @conn))))))))))

(defn- maybe-direct-local-scalar-update!
  [conn tx-data tx-meta]
  (when-let [prepared (db/prepare-scalar-update-tx ^DB @conn tx-data)]
    (when-let [report (direct-local-scalar-update! conn prepared tx-meta)]
      (observe-local-wal-tx-path! :scalar-update)
      report)))

(defn- maybe-direct-local-wal-transact!
  [conn tx-data tx-meta]
  (or
    (maybe-direct-local-scalar-update! conn tx-data tx-meta)
    (when *local-wal-patch-idoc?*
      (when-let [prepared (db/prepare-local-patch-idoc-tx ^DB @conn tx-data)]
        (when-let [report (direct-local-patch-idoc-transact!
                            conn prepared tx-meta)]
          (observe-local-wal-tx-path! :patch-idoc)
          report)))
    (when-let [prepared (db/prepare-blind-local-tx
                          ^DB @conn tx-data true false)]
      (when (:has-unique? prepared)
        (if (and *local-wal-identity-upsert?*
                 (:identity-upsert-av prepared))
          (when-let [[report upsert?]
                     (direct-local-identity-transact!
                       conn prepared tx-meta)]
            (observe-local-wal-tx-path!
              (if upsert? :identity-upsert :blind-insert))
            report)
          (when-let [report (direct-local-blind-transact!
                              conn prepared tx-meta)]
            (observe-local-wal-tx-path! :blind-insert)
            report))))))

(defn- single-identity-entity?
  "Cheap gate for the single-entity identity-upsert specialization: exactly one
   map entity naming a unique-identity attribute. Avoids blind preparation on
   the common write shapes that cannot use the specialization."
  [^DB db tx-data]
  (let [entities (seq tx-data)
        entity   (first entities)]
    (and entities
         (nil? (next entities))
         (map? entity)
         (boolean (some #(db/-is-attr? db % :db.unique/identity)
                        (keys entity))))))

(defn- maybe-direct-local-identity-transact!
  "Try the simple single-entity identity-upsert fast path for a store that is
   not on the WAL direct path. Returns the transaction report, or nil so the
   caller can continue with blind or general resolution."
  [conn tx-data tx-meta]
  (when (and *local-wal-identity-upsert?*
             (single-identity-entity? @conn tx-data))
    (when-let [prepared (db/prepare-blind-local-tx
                          ^DB @conn tx-data true false)]
      (when (:identity-upsert-av prepared)
        (when-let [[report upsert?]
                   (direct-local-identity-transact! conn prepared tx-meta)]
          (observe-local-wal-tx-path!
            (if upsert? :identity-upsert :blind-insert))
          report)))))

(declare current-thread-holds-store-write-lock?)

(defn- local-wal-transact-eligible?
  [conn]
  (let [db    ^DB @conn
        store (.-store db)]
    (and (instance? Store store)
         (let [lmdb (.-lmdb ^Store store)]
           (and (true? (:wal? (l/read-env-opts lmdb)))
                (not (l/writing? lmdb))
                (not (current-thread-holds-store-write-lock? store)))))))

(defn- prepared-local-wal-transact!
  [conn tx-data tx-meta]
  (locking conn
    (let [db    ^DB @conn
          store (.-store db)
          old   (db/cache-disabled? store)]
      (db/disable-cache store)
      (try
        (let [report (u/repeat-try-catch
                       c/+in-tx-overflow-times+
                       l/resized?
                       ;; Only datoms and report metadata cross into the writer.
                       ;; Keep preparation isolated without building a simulated
                       ;; read view that the commit path immediately discards.
                       (db/prepare-local-tx-data
                         (db/->TxReport db (db/transfer db store) [] {} tx-meta)
                         tx-data))]
          (with-transaction [c conn]
            (assert (active-conn-structural? c))
            (db/commit-prepared-tx-data! @c (:tx-data report) report))
          (observe-local-wal-tx-path! :general)
          (assoc report :db-after @conn))
        (finally
          (when-not old
            (db/enable-cache (.-store ^DB @conn))))))))

(defn- standalone-remote-transaction?
  [conn]
  (let [store (.-store ^DB @conn)]
    (and (instance? DatalogStore store) (not (l/writing? store)))))

(defn- direct-remote-transact!
  [conn tx-data tx-meta]
  (locking conn
    (assert (active-conn-structural? conn))
    (let [db     ^DB @conn
          report (with-isolated-tx-cache db tx-data tx-meta false)
          after  (db/carry-runtime-opts (:db-after report) db)]
      (reset! conn after)
      (assoc report :db-after after))))

(defn- commit-writing-report!
  ([db report ordered? path]
   (commit-writing-report! db report ordered? path false))
  ([db report ordered? path fused?]
    (let [preparation txcommon/*batch-prepare*
          ;; Add-only native batches already select ordered index passes. Avoid
          ;; forcing eager encoding when the collector can freeze ordinary datoms.
          ordered? (and ordered? (nil? preparation))]
      (when (and preparation (not fused?))
        (when (some #(identical? :db.cardinality/many
                                (:db/cardinality ((i/schema (:store db)) (:a %))))
                    (:tx-data report))
          (txcommon/materialize-scalar-pending! preparation db))
        ;; Simple requests build the hash view at the next request boundary.
        ;; General bodies materialize before running and retain sorted indexes.
        (when (txcommon/retain-pending-index? preparation)
          (doseq [datom (:tx-data report)]
            (txcommon/stage-batch-datom! (:db-after report) datom))))
      (if (and preparation
               (not fused?)
               (not s/*enforce-blind-unique-inserts?*)
               (= c/*ordered-datom-writes?* ordered?))
        (db/commit-prepared-tx-data! (:db-after report) (:tx-data report) report)
        (binding [s/*enforce-blind-unique-inserts?* fused?
                  c/*ordered-datom-writes?* ordered?]
          (db/commit-prepared-tx-data! (:db-after report) (:tx-data report) report)))
      ;; Publish resolver state only after conditional native inserts succeed.
      (when (and preparation fused?)
        (if (txcommon/retain-pending-index? preparation)
          (doseq [datom (:tx-data report)]
            (txcommon/stage-batch-datom! (:db-after report) datom))
          ;; The eager conditional put also drained preceding frozen writes.
          ;; Subsequent simple requests can read LMDB without rebuilding hashes.
          (txcommon/discard-scalar-pending! preparation))))
    (observe-local-wal-tx-path! path)
    (if (identical? (:db-before report) db)
      report
      (assoc report :db-before db))))

(defn- ^:redef transact-local-in-write-txn!
  "Apply one request to an already owned local writer. Prepare and resolve
  against preceding writes, retaining the general interpreter for other shapes."
  [^DB db tx-data tx-meta]
  ;; The collector retains resolver indexes until frozen writes are applied.
  ;; Other native transactions continue to isolate each request's indexes.
  (let [db1 (if txcommon/*batch-prepare*
              db (db/transfer db (.-store db)))
        ;; Both collectors own one native writer. Conditional inserts can
        ;; roll back their reservations before falling back to upsert resolution.
        fuse-unique? (and (not (:client-op/id tx-meta))
                          (or txcommon/*batch-prepare*
                              (s/current-write-group
                                (.-lmdb ^Store (.-store db)))))]
    (or
      (when-let [prepared (db/prepare-scalar-update-tx db1 tx-data)]
        (when-let [report (db/stamp-scalar-update-tx db1 prepared tx-meta)]
          (commit-writing-report! db report false :scalar-update)))
      (when *local-wal-patch-idoc?*
        (when-let [prepared (db/prepare-local-patch-idoc-tx db1 tx-data)]
          (when-let [{:keys [report]}
                     (db/stamp-local-patch-idoc-tx db1 prepared tx-meta)]
            (commit-writing-report! db report false :patch-idoc))))
      (when-let [prepared (db/prepare-blind-local-tx
                           db1 tx-data true
                           (not fuse-unique?))]
        (if (and *local-wal-identity-upsert?* (:identity-upsert-av prepared))
          (when-let [[report upsert?]
                     (db/stamp-blind-local-identity-tx db1 prepared tx-meta)]
            (commit-writing-report! db report (not upsert?)
                                   (if upsert? :identity-upsert :blind-insert)))
          (if (and fuse-unique? (:fuse-unique-inserts? prepared))
            (try
              (commit-writing-report! db (db/stamp-blind-local-tx db1 prepared tx-meta)
                                     true :blind-insert true)
              (catch Exception e
                (when-not (and (l/blind-unique-collision? e)
                               (:unique-inserts-rolled-back? (ex-data e)))
                  (throw e))))
            (when (db/blind-local-tx-unique-values-absent? db1 prepared)
              (commit-writing-report! db (db/stamp-blind-local-tx db1 prepared tx-meta)
                                     true :blind-insert)))))
      (do
        (when-let [preparation txcommon/*batch-prepare*]
          (txcommon/materialize-scalar-pending! preparation db1))
        (let [report (db/transact-tx-data (db/->TxReport db db1 [] {} tx-meta)
                                          tx-data false)]
          (observe-local-wal-tx-path! :general)
          report)))))

(defn- -transact! [conn tx-data tx-meta]
  (if (local-direct-transact-eligible? conn)
    (or (maybe-direct-local-identity-transact! conn tx-data tx-meta)
        (maybe-direct-local-scalar-update! conn tx-data tx-meta)
        (maybe-direct-local-blind-transact! conn tx-data tx-meta)
        (direct-local-transact! conn tx-data tx-meta))
    (if (local-wal-transact-eligible? conn)
      (or (maybe-direct-local-wal-transact! conn tx-data tx-meta)
          (prepared-local-wal-transact! conn tx-data tx-meta))
      (let [store (.-store ^DB @conn)]
        (if (and (instance? Store store)
                 (l/writing? (.-lmdb ^Store store))
                 (Thread/holdsLock (l/write-txn (.-lmdb ^Store store)))
                 (db/cache-disabled? store)
                 ;; Keep the nested watchdog when a global timeout is enabled.
                 ;; An explicit outer timeout is still enforced by its owner.
                 (nil? (l/explicit-transaction-timeout)))
          (locking conn
            ;; The enclosing transaction already owns this Store and writer.
            ;; Only the mutable datom overlays need isolation for preparation;
            ;; retain the resulting DB view for subsequent transaction-local
            ;; reads and writes. The owner commits and publishes the base view.
            (let [db     ^DB @conn
                  report (transact-local-in-write-txn! db tx-data tx-meta)
                  after  (db/carry-runtime-opts (:db-after report) db)]
              (reset! conn after)
              (assoc report :db-after after)))
          (let [report (with-transaction [c conn]
                         (assert (active-conn-structural? c))
                         (if (instance? Store (.-store ^DB @c))
                           (transact-local-in-write-txn! @c tx-data tx-meta)
                           (with @c tx-data tx-meta)))]
            (assoc report :db-after @conn)))))))

(defn- notify-listeners!
  [conn report]
  (when-not (datalog/defer-listeners! conn report)
    (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
      (callback report))))

(defn- run-transact-now!
  [conn tx-data tx-meta]
  (let [report (-transact! conn tx-data tx-meta)]
    (notify-listeners! conn report)
    report))

(def ^:dynamic *txlog-sync-path-observer* nil)

(defn- observe-txlog-sync-path!
  [path]
  (when (fn? *txlog-sync-path-observer*)
    (*txlog-sync-path-observer* path)))

(defn- current-thread-holds-store-write-lock?
  [store]
  (boolean
    (or
      (when-let [write-lock (l/write-txn store)]
        (Thread/holdsLock write-lock))
      (when (instance? Store store)
        (when-let [lmdb-write-lock (l/write-txn (.-lmdb ^Store store))]
          (Thread/holdsLock lmdb-write-lock))))))

(defn- embedded-write-group
  [conn]
  (when-not (Thread/holdsLock conn)
    (let [store (.-store ^DB @conn)]
      (when (and (instance? Store store)
                 (not (s/synchronous-secondary-indexing? store)))
        ;; A runner publishes only its owning connection. Connections can share
        ;; the environment while retaining different runtime options.
        (kv/write-group (.-lmdb ^Store store) [:datalog (conn-state conn)])))))

(defn- observe-embedded-path!
  [profile batched?]
  (observe-txlog-sync-path!
   (case profile
     :strict (if batched? :queued-strict :direct-wal-idle-strict)
     :relaxed (if batched? :queued-relaxed :direct-wal-idle-relaxed)
     :extra (if batched? :queued-extra :direct-wal-idle-extra)
     (if batched? :queued-no-wal :direct-no-wal))))

(defn- ensure-group-secondary-safe!
  [store]
  ;; Recheck under the native writer: indexing options can change after queue
  ;; admission. Retry individual requests before any secondary side effects.
  (when (and (> (group/request-count) 1)
             (s/synchronous-secondary-indexing? store))
    (throw (ex-info "Secondary indexing requires individual commits"
                    {:datalevin.tx-group/body-failure true}))))

(defn- transact-embedded-group!
  [g conn tx-data tx-meta]
  (let [lmdb (.-lmdb ^Store (.-store ^DB @conn))
        op (fn [[tx batched?]]
             (observe-embedded-path! nil batched?)
             (-transact! tx tx-data tx-meta))
        report
        (group/submit!
         g
         (fn [execute]
           (if-not (group/batched?)
             (execute [conn false])
             (locking conn
               (let [before @conn
                     ^objects reports
                     (locking (l/write-txn lmdb)
                       (with-transaction [tx conn]
                         (group/collect! execute)
                         (when (s/synchronous-secondary-indexing? (.-store ^DB @tx))
                           (group/collect! execute true))
                         (ensure-group-secondary-safe! (.-store ^DB @tx))
                         (:result (db/execute-write-group
                                   tx #(execute [% (> (group/request-count) 1)])))))
                     after @conn
                     store (.-store ^DB after)]
                 ;; The native writer is closed. Return readable Store views,
                 ;; retaining each request's logical transaction and tempids.
                 (dotimes [idx (alength reports)]
                   (let [report (aget reports idx)]
                     (aset reports idx
                           (assoc report
                                  :db-before (if (zero? idx) before
                                                 (db/transfer (:db-before report)
                                                              store))
                                  :db-after after))))
                 reports))))
         op
         (kv/write-batch-delay-nanos lmdb))]
    (notify-listeners! conn report)
    report))

(defn- stamp-collected-scalar!
  [control ^DB before prepared tx-meta]
  (when *txlog-sync-path-observer*
    (let [opts (l/read-env-opts (.-lmdb ^Store (.-store before)))]
      (observe-embedded-path! (:wal-durability-profile opts)
                              (datalog/batched? control))))
  (when-let [report (db/stamp-scalar-update-tx before prepared tx-meta)]
    (commit-writing-report! before report false :scalar-update)))

(defn transact!
  ([conn tx-data] (transact! conn tx-data nil))
  ([conn tx-data tx-meta]
   ;; Remote preparation must run under the server's writer, including when
   ;; callers share a connection. Client-side simulated batching could otherwise
   ;; race transactions submitted by another client between prepare and commit.
   (if (standalone-remote-transaction? conn)
     (let [report (direct-remote-transact! conn tx-data tx-meta)]
       (observe-txlog-sync-path! :direct-remote)
       (notify-listeners! conn report)
       report)
     (if-let [control (datalog/control conn)]
       (let [opts (l/read-env-opts (.-lmdb ^Store (:store @conn)))
             ;; Type correction and read-layout construction do not depend on
             ;; the eventual native snapshot. Keep that work on the caller;
             ;; resolve lookup refs and read old values on the native owner.
             prepared (db/prepare-scalar-update-tx
                        @conn tx-data {:defer-entity-resolution? true})
             report (if (and prepared (nil? (l/explicit-transaction-timeout)))
                      (datalog/run-scalar! control conn prepared tx-data tx-meta
                                           stamp-collected-scalar! -transact!)
                      (datalog/with-transaction!
                        control conn
                        (fn [tx]
                          (observe-embedded-path! (:wal-durability-profile opts)
                                                  (datalog/batched? control))
                          (or (when prepared
                                (when-let [report (db/stamp-scalar-update-tx
                                                    @tx prepared tx-meta)]
                                  (let [report (commit-writing-report!
                                                 @tx report false :scalar-update)]
                                    (reset! tx (:db-after report))
                                    report)))
                              (-transact! tx tx-data tx-meta)))
                        {:context {:datalog-prepare? true :datalog-fast? true}}))]
         (notify-listeners! conn report)
         report)
     (if-let [g (embedded-write-group conn)]
       (transact-embedded-group! g conn tx-data tx-meta)
       (do
         (let [store (.-store ^DB @conn)
               opts (when (and (instance? Store store)
                               (not (l/writing? (.-lmdb ^Store store)))
                               (not (current-thread-holds-store-write-lock? store)))
                      (l/read-env-opts (.-lmdb ^Store store)))]
           (observe-embedded-path!
            (when (:wal? opts) (:wal-durability-profile opts)) false))
         (run-transact-now! conn tx-data tx-meta)))))))

(defn transact-ack!
  ([conn tx-data] (transact-ack! conn tx-data nil))
  ([conn tx-data tx-meta]
   (if (standalone-remote-transaction? conn)
     (let [report (locking conn
                    (assert (active-conn-structural? conn))
                    (if (seq (some-> (:listeners (meta conn)) deref))
                      (direct-remote-transact! conn tx-data tx-meta)
                      (do (reset! conn (db/transact-ack @conn tx-data))
                          nil)))]
       (observe-txlog-sync-path! :direct-remote)
       (when report (notify-listeners! conn report)))
     ;; Retain local queueing and explicit transaction/watchdog semantics.
     (transact! conn tx-data tx-meta))
   :transacted))

(defn reset-conn!
  ([conn db] (reset-conn! conn db nil))
  ([conn db tx-meta]
   (let [report (db/map->TxReport
                  {:db-before @conn
                   :db-after  db
                   :tx-data   (let [ds (db/-datoms db :eav nil nil nil)]
                                (u/concatv
                                  (mapv #(assoc % :added false) ds)
                                  ds))
                   :tx-meta   tx-meta})]
     (reset! conn db)
     (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
       (callback report))
     db)))

(defn- atom? [a] (instance? clojure.lang.IAtom a))

(defn listen!
  ([conn callback] (listen! conn (rand) callback))
  ([conn key callback]
   {:pre [(conn? conn) (atom? (:listeners (meta conn)))]}
   ;; Registration after an acknowledgement-only transaction begins applies
   ;; to the next transaction, which can fetch a report for this listener.
   (locking conn
     (swap! (:listeners (meta conn)) assoc key callback))
   key))

(defn unlisten!
  [conn key]
  {:pre [(conn? conn) (atom? (:listeners (meta conn)))]}
  (swap! (:listeners (meta conn)) dissoc key))

(defn listen-db!
  ([conn callback] (listen-db! conn (random-uuid) callback))
  ([conn key callback]
   {:pre [(conn? conn) (ifn? callback)]}
   (let [store (.-store ^DB @conn)
         listeners (:db-listeners (meta conn))]
     (when-not (instance? DatalogStore store)
       (raise "Database subscriptions require a remote Datalog connection"
              {:error :notification/remote-required}))
     (when-not (atom? listeners)
       (raise "Connection does not support database subscriptions" {}))
     (let [stop! (r/listen-db store callback)]
       (try
         (let [[before _] (swap-vals!
                           listeners
                           #(if (nil? %)
                              (raise "Connection is closed" {})
                              (assoc % key stop!)))]
           (when-let [previous (get before key)] (previous)))
         (catch Throwable t
           (stop!)
           (throw t))))
     key)))

(defn unlisten-db!
  [conn key]
  (when-let [listeners (:db-listeners (meta conn))]
    (let [[before _] (swap-vals! listeners #(when % (dissoc % key)))]
      (when-let [stop! (get before key)] (stop!))))
  nil)

(defn db
  [conn]
  {:pre [(conn? conn)]}
  @conn)

(defn opts
  [conn]
  (let [store (.-store ^DB @conn)
        opts  (i/opts store)]
    (when (instance? DatalogStore store)
      (alter-conn-meta! conn assoc
                        :remote-store-opts-cache
                        (c/canonicalize-wal-opts opts)))
    opts))

(defn schema
  "Return the schema of Datalog DB"
  [conn]
  {:pre [(conn? conn)]}
  (i/schema ^Store (.-store ^DB @conn)))

(defn secondary-index-status
  [conn]
  {:pre [(conn? conn)]}
  (db/secondary-index-status ^DB @conn))

(defn process-secondary-index-jobs!
  ([conn]
   (process-secondary-index-jobs! conn nil))
  ([conn opts]
   {:pre [(conn? conn)]}
   (db/process-secondary-index-jobs! ^DB @conn opts)))

(defn wait-for-secondary-index
  ([conn]
   (wait-for-secondary-index conn nil))
  ([conn opts]
   {:pre [(conn? conn)]}
   (db/wait-for-secondary-index ^DB @conn opts)))

(defn update-schema
  ([conn schema-update]
   (update-schema conn schema-update nil nil))
  ([conn schema-update del-attrs]
   (update-schema conn schema-update del-attrs nil))
  ([conn schema-update del-attrs rename-map]
   {:pre [(conn? conn)]}
   (vld/validate-schema-update schema-update)
   (let [^DB db       (db conn)
         ^Store store (.-store db)
         result       (i/set-schema store schema-update del-attrs rename-map)]
     ;; A value-type update can re-encode stored datoms without going through
     ;; the normal Datalog transaction cache invalidation path.
     (db/refresh-cache store)
     result)))

(defn index-attr
  [conn attr]
  {:pre [(conn? conn)]}
  (locking conn
    (let [store (.-store ^DB (db conn))
          result (i/index-attr store attr)]
      (db/refresh-cache store)
      result)))

(defonce ^:private connections (atom {}))
(defonce ^:private transact-async-executor-atom (atom nil))

(defn- add-conn [dir conn] (swap! connections assoc dir conn))

(defn- remove-conn
  [dir conn]
  (when dir
    (swap! connections
           (fn [m]
             (if (identical? (get m dir) conn)
               (dissoc m dir)
               m))))
  nil)

(defn- new-conn
  [dir schema opts]
  (let [conn (create-conn dir schema opts)]
    (alter-conn-meta! conn assoc :dir dir)
    (add-conn dir conn)
    conn))

(defn- new-transact-async-executor
  []
  (let [threads (.availableProcessors (Runtime/getRuntime))
        workers (ThreadPoolExecutor.
                  threads threads 0 TimeUnit/MILLISECONDS
                  (ArrayBlockingQueue. (* 4 threads))
                  (ThreadPoolExecutor$CallerRunsPolicy.))
        executor (a/->AsyncExecutor (Executors/newSingleThreadExecutor)
                                    workers
                                    (LinkedBlockingQueue.)
                                    (ConcurrentHashMap.)
                                    (AtomicBoolean. false)
                                    (a/new-backlog-semaphore))]
    (a/start executor)
    executor))

(defn- get-transact-async-executor
  []
  (locking transact-async-executor-atom
    (let [executor @transact-async-executor-atom]
      (if (and executor (a/running? executor))
        executor
        (let [executor (new-transact-async-executor)]
          (reset! transact-async-executor-atom executor)
          executor)))))

(defn ^:no-doc shutdown-transact-async-executor!
  []
  (locking transact-async-executor-atom
    (when-let [executor @transact-async-executor-atom]
      (a/stop executor)
      (reset! transact-async-executor-atom nil)))
  nil)

(defn ^:no-doc shutdown-transact-async-executor-if-idle!
  []
  (locking transact-async-executor-atom
    (when-let [^AsyncExecutor executor @transact-async-executor-atom]
      (let [^LinkedBlockingQueue event-queue (.-event-queue executor)
            workers                         (.-workers executor)
            ^ThreadPoolExecutor worker-pool (when (instance? ThreadPoolExecutor workers)
                                               workers)
            workers-idle?                   (if worker-pool
                                             (and (zero? (.getActiveCount worker-pool))
                                                  (.isEmpty (.getQueue worker-pool)))
                                             true)]
        (when (and (.isEmpty event-queue) workers-idle?)
          (a/stop executor)
          (reset! transact-async-executor-atom nil)))))
  nil)

(defn get-conn
  ([dir]
   (get-conn dir nil nil))
  ([dir schema]
   (get-conn dir schema nil))
  ([dir schema opts]
   (if (and (map? opts) (contains? opts :runtime-opts))
     (create-conn dir schema opts)
     (if-let [c (get @connections dir)]
       (if (closed? c) (new-conn dir schema opts) c)
       (new-conn dir schema opts)))))

(defmacro with-conn
  "Evaluate body in the context of an connection to the Datalog database.

  If the database does not exist, this will create it. If it is closed,
  this will open it. However, the connection will be closed in the end of
  this call. If a database needs to be kept open, use `create-conn` and
  hold onto the returned connection. See also [[create-conn]] and [[get-conn]]

  `spec` is a vector of an identifier of new database connection, a path or
  dtlv URI string, a schema map and a option map. The last two are optional.

  Example:

          (with-conn [conn \"my-data-path\"]
            ;; body)

          (with-conn [conn \"my-data-path\" {:likes {:db/cardinality :db.cardinality/many}}]
            ;; body)
  "
  [spec & body]
  `(let [r#      (list ~@(rest spec))
         dir#    (first r#)
         schema# (second r#)
         opts#   (second (rest r#))
         conn#   (get-conn dir# schema# opts#)]
     (try
       (let [~(first spec) conn#] ~@body)
       (finally (close conn#)))))

(declare dl-tx-combine)

(defn- dl-work-key* [db-name] (->> db-name hash (str "tx") keyword))

(def ^:no-doc dl-work-key (memoize dl-work-key*))
(defn- tx-data-size
  ^long [tx-data]
  (if (instance? java.util.Collection tx-data)
    (.size ^java.util.Collection tx-data)
    (count tx-data)))

(deftype ^:no-doc AsyncDLTx [conn tx-data tx-meta cb]
  IAsyncWork
  (work-key [_] (->> (.-store ^DB @conn) i/db-name dl-work-key))
  ;; Async transact stays at the API layer and delegates execution to transact!.
  (do-work [_] (transact! conn tx-data tx-meta))
  (combine [_] dl-tx-combine)
  (callback [_] cb)
  IBoundedAsyncWork
  (batch-weight [_] (tx-data-size tx-data))
  (max-batch-weight [_] c/*datalog-async-batch-max-forms*))

(defn- add-combined-tx-data!
  [^FastList out tx-data]
  (if (instance? java.util.Collection tx-data)
    (.addAll out ^java.util.Collection tx-data)
    (doseq [tx tx-data]
      (.add out tx)))
  out)

(defn- dl-tx-combine
  [coll]
  (let [^AsyncDLTx fw (first coll)]
    (if (nil? (next coll))
      fw
      (let [capacity (reduce (fn [^long n ^AsyncDLTx work]
                               (+ n (tx-data-size (.-tx-data work))))
                             0
                             coll)
            ^FastList out (FastList. (int capacity))]
        (doseq [^AsyncDLTx work coll]
          (add-combined-tx-data! out (.-tx-data work)))
        (->AsyncDLTx (.-conn fw)
                     out
                     (.-tx-meta fw)
                     (.-cb fw))))))

(defn transact-async
  ([conn tx-data] (transact-async conn tx-data nil))
  ([conn tx-data tx-meta] (transact-async conn tx-data tx-meta nil))
  ([conn tx-data tx-meta callback]
   (a/exec (get-transact-async-executor)
           (->AsyncDLTx conn tx-data tx-meta callback))))

(defn transact
  ([conn tx-data] (transact conn tx-data nil))
  ([conn tx-data tx-meta]
   {:pre [(conn? conn)]}
   (let [fut (transact-async conn tx-data tx-meta)]
     @fut
     fut)))

(defn open-kv
  "it's here to access remote ns"
  ([dir]
   (open-kv dir nil))
  ([dir opts]
   (if (u/dtlv-uri? dir)
     (r/open-kv dir opts)
     ((requiring-resolve 'datalevin.tx-group.batch.embedded/attach!)
      (l/open-kv dir opts)))))

(defmacro with-kv
  "Evaluate body with an opened KV database, then close it.

  If the KV database does not exist, this will create it. The KV handle is
  closed at the end of this call. If a KV database needs to be kept open, use
  `open-kv` and hold onto the returned handle. See also [[open-kv]].

  `spec` is a vector of an identifier of the KV handle, a path or dtlv URI
  string, and an optional option map.

  Example:

          (with-kv [db \"my-data-path\"]
            ;; body)

          (with-kv [db \"my-data-path\" {:wal? true}]
            ;; body)
  "
  [spec & body]
  `(let [r#    (list ~@(rest spec))
         dir#  (first r#)
         opts# (second r#)
         db#   (open-kv dir# opts#)]
     (try
       (let [~(first spec) db#] ~@body)
       (finally (i/close-kv db#)))))

(defn clear
  "Close the Datalog database, then clear all data, including schema."
  [conn]
  (let [store (.-store ^DB @conn)
        lmdb  (if (instance? DatalogStore store)
                (let [dir (i/dir store)]
                  (close conn)
                  (open-kv dir))
                (.-lmdb ^Store store))]
    (try
      (doseq [dbi [c/eav c/ave c/giants c/ha-client-ops c/schema c/meta]]
        (i/clear-dbi lmdb dbi))
      (finally
        (db/remove-cache store)
        (i/close-kv lmdb)))))
