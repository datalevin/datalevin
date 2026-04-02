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
   [datalevin.lmdb :as l]
   [datalevin.storage :as s]
   [datalevin.async :as a]
   [datalevin.remote :as r]
   [datalevin.util :as u]
   [datalevin.interface :as i]
   [datalevin.validate :as vld])
  (:import
   [datalevin.db DB TxReport]
   [datalevin.storage Store]
   [datalevin.remote DatalogStore]
   [datalevin.async IAsyncWork AsyncExecutor]
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util.concurrent Executors LinkedBlockingQueue ConcurrentHashMap
    ThreadPoolExecutor ArrayBlockingQueue ThreadPoolExecutor$CallerRunsPolicy
    TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]))

(declare close closed? remove-conn shutdown-transact-async-executor!
         shutdown-transact-async-executor-if-idle!)

(defonce ^:private shared-local-stores (atom {}))

(defn conn?
  [conn]
  (and (instance? clojure.lang.IDeref conn) (db/db? @conn)))

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
  (wrap-conn
   (atom db :meta {:listeners (atom {})
                   :runtime-opts (db/runtime-opts db)
                   :sync-queue-pending (AtomicLong. 0)
                   :sync-queue-last-enqueue-ms (AtomicLong. 0)})))

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
  (when (and (string? dir) (not (r/dtlv-uri? dir)))
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
  (vld/validate-schema schema)
  (let [[store-opts runtime-opts] (split-runtime-opts opts)]
    (if-let [dir-key (shared-local-store-key dir)]
      (let [store (acquire-shared-local-store dir schema store-opts)]
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

(defmacro with-transaction
  "Evaluate body within the context of a single new read/write transaction,
  ensuring atomicity of Datalog database operations. Works with synchronous
  `transact!`.

  `conn` is a new identifier of the Datalog database connection with a new
  read/write transaction attached, and `orig-conn` is the original database
  connection.

  `body` should refer to `conn`.

  Example:

          (with-transaction [cn conn]
            (let [query  '[:find ?c .
                           :in $ ?e
                           :where [?e :counter ?c]]
                  ^long now (q query @cn 1)]
              (transact! cn [{:db/id 1 :counter (inc now)}])
              (q query @cn 1))) "
  [[conn orig-conn] & body]
  `(locking ~orig-conn
     (let [db#  ^DB (deref ~orig-conn)
           s#   (.-store db#)
           old# (db/cache-disabled? s#)]
       (db/disable-cache s#)
       (if (instance? DatalogStore s#)
         (locking (l/write-txn s#)
           (let [res#    (if (l/writing? s#)
                           (let [~conn ~orig-conn]
                             ~@body)
                           (let [s1# (r/open-transact s#)
                                 w#  #(let [~conn
                                            (atom (db/transfer db# s1#)
                                                  :meta (meta ~orig-conn))]
                                        ~@body) ]
                             (try
                               (u/repeat-try-catch
                                   ~c/+in-tx-overflow-times+
                                   l/resized? (w#))
                               (finally (r/close-transact s#)))))
                 new-db# (db/carry-runtime-opts (db/new-db s#) db#)]
             (reset! ~orig-conn new-db#)
             (when-not old# (db/enable-cache s#))
             res#))
         (let [kv#     (.-lmdb ^Store s#)
               s1#     (volatile! nil)
               res1#   (l/with-transaction-kv [kv1# kv#]
                         (let [conn1# (atom (db/transfer
                                              db# (s/transfer s# kv1#))
                                            :meta (meta ~orig-conn))
                               res#   (let [~conn conn1#]
                                        ~@body)]
                           (vreset! s1# (.-store ^DB (deref conn1#)))
                           res#))
               new-s#  (s/transfer (deref s1#) kv#)
               new-db# (db/carry-runtime-opts (db/new-db new-s#) db#)]
           (reset! ~orig-conn new-db#)
           (when-not old# (db/enable-cache new-s#))
           res1#)))))

(defn with
  ([db tx-data] (with db tx-data {} false))
  ([db tx-data tx-meta] (with db tx-data tx-meta false))
  ([db tx-data tx-meta simulated?]
   (db/transact-tx-data (db/->TxReport db db [] {} tx-meta)
                        tx-data simulated?)))

(defn db-with
  [db tx-data]
  (:db-after (with db tx-data)))

(defn- local-direct-transact-eligible?
  [conn]
  (let [store (.-store ^DB @conn)]
    (and (instance? Store store)
         (let [lmdb (.-lmdb ^Store store)]
           ;; In WAL mode, route through with-transaction-kv so transaction
           ;; boundaries are explicitly anchored to LMDB write transactions.
           (and (not (true? (:wal? (i/env-opts lmdb))))
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
                       (with db tx-data tx-meta))]
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
            (let [kv            (.-lmdb store)
                  prepared-store (volatile! nil)
                  [report info]
                  (l/with-transaction-kv [kv1 kv]
                    (let [store1 ^Store (s/transfer store kv1)
                          db1    ^DB    (db/transfer db store1)]
                      (vreset! prepared-store store1)
                      (let [[report info] (db/stamp-blind-local-tx
                                            db1 prepared tx-meta)]
                        (db/commit-prepared-tx-data! db1 (:tx-data report))
                        [report info])))
                  new-store      ^Store (s/transfer ^Store @prepared-store kv)
                  new-info       (assoc info :last-modified
                                        (long (or (i/last-modified new-store)
                                                  0)))
                  new-db         (db/carry-runtime-opts
                                   (db/new-db new-store new-info)
                                   db)]
              (reset! conn new-db)
              (assoc report :db-after @conn))
            (finally
              (when-not old
                (db/enable-cache (.-store ^DB @conn))))))))))

(defn- maybe-direct-local-blind-transact!
  [conn tx-data tx-meta]
  (when-let [prepared (db/prepare-blind-local-tx ^DB @conn tx-data)]
    (direct-local-blind-transact! conn prepared tx-meta)))

(defn- -transact! [conn tx-data tx-meta]
  (if (local-direct-transact-eligible? conn)
    (or (maybe-direct-local-blind-transact! conn tx-data tx-meta)
        (direct-local-transact! conn tx-data tx-meta))
    (let [report (with-transaction [c conn]
                   (assert (active-conn-structural? c))
                   (with @c tx-data tx-meta))]
      (assoc report :db-after @conn))))

(defn- notify-listeners!
  [conn report]
  (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
    (callback report)))

(defn- run-transact-now!
  [conn tx-data tx-meta]
  (let [report (-transact! conn tx-data tx-meta)]
    (notify-listeners! conn report)
    report))

(def ^:dynamic *sync-queue-worker?* false)
(def ^:dynamic *txlog-sync-path-observer* nil)

(defn- observe-txlog-sync-path!
  [path]
  (when (fn? *txlog-sync-path-observer*)
    (*txlog-sync-path-observer* path)))

(defn- ensure-sync-queue-state!
  [conn]
  (let [m (meta conn)
        queued-pending (:sync-queue-pending m)
        last-enqueue-ms (:sync-queue-last-enqueue-ms m)
        pending* (if (instance? AtomicLong queued-pending)
                   queued-pending
                   (AtomicLong. 0))
        last-enqueue-ms* (if (instance? AtomicLong last-enqueue-ms)
                           last-enqueue-ms
                           (AtomicLong. 0))]
    (when (or (not (identical? pending* queued-pending))
              (not (identical? last-enqueue-ms* last-enqueue-ms)))
      (alter-conn-meta! conn assoc
                        :sync-queue-pending pending*
                        :sync-queue-last-enqueue-ms last-enqueue-ms*))
    {:queued-pending pending*
     :last-enqueue-ms last-enqueue-ms*}))

(defn- sync-queue-pending-counter
  [conn]
  ^AtomicLong (:queued-pending (ensure-sync-queue-state! conn)))

(defn- sync-queue-last-enqueue-ms-counter
  [conn]
  ^AtomicLong (:last-enqueue-ms (ensure-sync-queue-state! conn)))

(defn- queue-pending-dec-by!
  [conn n]
  (let [^AtomicLong pending (sync-queue-pending-counter conn)
        after (.addAndGet pending (- (long n)))]
    (when (neg? after)
      (.set pending 0))))

(defn- current-thread-holds-store-write-lock?
  [store]
  (boolean
    (or
      (when-let [write-lock (l/write-txn store)]
        (Thread/holdsLock write-lock))
      (when (instance? Store store)
        (when-let [lmdb-write-lock (l/write-txn (.-lmdb ^Store store))]
          (Thread/holdsLock lmdb-write-lock))))))

(defn- wal-sync-queue-profile-from-opts
  [opts]
  (let [opts    (c/canonicalize-wal-opts opts)
        profile (or (:wal-durability-profile opts)
                    c/*wal-durability-profile*)]
    (when (and (true? (:wal? opts)) profile)
      profile)))

(defn- cached-remote-store-opts
  [conn ^DatalogStore store]
  (if-some [entry (find (meta conn) :remote-store-opts-cache)]
    (val entry)
    (let [opts (c/canonicalize-wal-opts (i/opts store))]
      (alter-conn-meta! conn assoc :remote-store-opts-cache opts)
      opts)))

(defn- txlog-sync-queue-profile
  [conn]
  (when (and (not *sync-queue-worker?*)
             (instance? clojure.lang.IDeref conn))
    (let [db @conn]
      ;; `conn?` refreshes remote cache state via `last-modified`, which is
      ;; unnecessary for picking the local sync path and can stall writes
      ;; during HA failover when leadership is still converging.
      (when (instance? DB db)
        (let [store (.-store ^DB db)]
          (when (and (not (current-thread-holds-store-write-lock? store))
                     (or (and (instance? Store store)
                              (not (l/writing? (.-lmdb ^Store store))))
                         (and (instance? DatalogStore store)
                              (not (l/writing? store)))))
            (cond
              (instance? Store store)
              (wal-sync-queue-profile-from-opts
                (i/env-opts (.-lmdb ^Store store)))

              (instance? DatalogStore store)
              (wal-sync-queue-profile-from-opts
                (cached-remote-store-opts conn store))

              :else nil)))))))

(defn- strict-txlog-sync-queue?
  [conn]
  (= :strict (txlog-sync-queue-profile conn)))

(declare queued-transact!)

(def ^:private wal-idle-direct-cooldown-ms
  5)

(defn- try-direct-wal-transact-when-idle!
  [conn tx-data tx-meta]
  (let [now-ms (System/currentTimeMillis)
        ^AtomicLong last-enqueue-ms (sync-queue-last-enqueue-ms-counter conn)]
    (if (< (- now-ms (.get last-enqueue-ms))
           (long wal-idle-direct-cooldown-ms))
      [false nil]
      (let [^AtomicLong pending (sync-queue-pending-counter conn)]
        (if (.compareAndSet pending 0 1)
          (try
            [true (run-transact-now! conn tx-data tx-meta)]
            (finally
              (queue-pending-dec-by! conn 1)))
          [false nil])))))

(defn transact!
  ([conn tx-data] (transact! conn tx-data nil))
  ([conn tx-data tx-meta]
   (let [profile (txlog-sync-queue-profile conn)]
     (cond
       (nil? profile)
       (do
         (observe-txlog-sync-path! :direct-no-wal)
         (run-transact-now! conn tx-data tx-meta))

       (= :strict profile)
       ;; Strict durability can take the idle direct fast path when there is no
       ;; queue pressure, but falls back to the sync queue adaptively once work
       ;; is already pending.
       (let [[direct? report]
             (try-direct-wal-transact-when-idle! conn tx-data tx-meta)]
         (if direct?
           (do
             (observe-txlog-sync-path! :direct-wal-idle-strict)
             report)
           (do
             (observe-txlog-sync-path! :queued-strict)
             (queued-transact! conn tx-data tx-meta))))

       (= :relaxed profile)
       ;; Relaxed durability relies on the sync queue to batch fsync work.
       ;; Allowing an idle direct fast path defeats the expected batching
       ;; semantics and makes the behavior diverge from the documented model.
       (do
         (observe-txlog-sync-path! :queued-relaxed)
         (queued-transact! conn tx-data tx-meta))

       (= :extra profile)
       ;; Extra durability follows the same adaptive dispatch as strict, with a
       ;; stricter sync primitive on the durability side.
       (let [[direct? report]
             (try-direct-wal-transact-when-idle! conn tx-data tx-meta)]
         (if direct?
           (do
             (observe-txlog-sync-path! :direct-wal-idle-extra)
             report)
           (do
             (observe-txlog-sync-path! :queued-extra)
             (queued-transact! conn tx-data tx-meta))))

       :else
       (let [[direct? report]
             (try-direct-wal-transact-when-idle! conn tx-data tx-meta)]
         (if direct?
           (do
             (observe-txlog-sync-path! :direct-wal-idle-other)
             report)
           (do
             (observe-txlog-sync-path! :queued-other)
             (queued-transact! conn tx-data tx-meta))))))))

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
   (swap! (:listeners (meta conn)) assoc key callback)
   key))

(defn unlisten!
  [conn key]
  {:pre [(conn? conn) (atom? (:listeners (meta conn)))]}
  (swap! (:listeners (meta conn)) dissoc key))

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

(defn update-schema
  ([conn schema-update]
   (update-schema conn schema-update nil nil))
  ([conn schema-update del-attrs]
   (update-schema conn schema-update del-attrs nil))
  ([conn schema-update del-attrs rename-map]
   {:pre [(conn? conn)]}
   (when schema-update (vld/validate-schema schema-update))
   (let [^DB db       (db conn)
         ^Store store (.-store db)]
     (i/set-schema store schema-update)
     (doseq [attr del-attrs] (i/del-attr store attr))
     (doseq [[old new] rename-map] (i/rename-attr store old new))
     (schema conn))))

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

(declare dl-tx-combine
         transact!
         sync-queued-dl-tx-combine
         run-sync-queued-dl-batch!)

(defn- dl-work-key* [db-name] (->> db-name hash (str "tx") keyword))

(def ^:no-doc dl-work-key (memoize dl-work-key*))
(defn- sync-queued-dl-work-key* [db-name] (->> db-name hash (str "tx-sync") keyword))
(def ^:private sync-queued-dl-work-key (memoize sync-queued-dl-work-key*))

(deftype ^:no-doc SyncQueuedReq [tx-data tx-meta result-promise])
(deftype ^:no-doc SyncQueuedResult [report error])

(deftype ^:no-doc AsyncDLTx [conn tx-data tx-meta cb]
  IAsyncWork
  (work-key [_] (->> (.-store ^DB @conn) i/db-name dl-work-key))
  ;; Async transact stays at the API layer and delegates execution to transact!.
  (do-work [_] (transact! conn tx-data tx-meta))
  (combine [_] dl-tx-combine)
  (callback [_] cb))

(deftype ^:no-doc SyncQueuedDLTx [conn requests]
  IAsyncWork
  (work-key [_] (->> (.-store ^DB @conn) i/db-name sync-queued-dl-work-key))
  (do-work [_] (run-sync-queued-dl-batch! conn requests))
  (combine [_] sync-queued-dl-tx-combine)
  (callback [_] nil))

(defn- dl-tx-combine
  [coll]
  (let [^AsyncDLTx fw (first coll)]
    (if (nil? (next coll))
      fw
      (->AsyncDLTx (.-conn fw)
                   (into [] (comp (map #(.-tx-data ^AsyncDLTx %)) cat) coll)
                   (.-tx-meta fw)
                   (.-cb fw)))))

(defn- sync-queued-dl-tx-combine
  [coll]
  (let [^SyncQueuedDLTx fw (first coll)]
    (if (nil? (next coll))
      fw
      (let [^FastList out (FastList.)]
        (doseq [^SyncQueuedDLTx work coll]
          (.addAll out ^java.util.Collection (.-requests work)))
        (->SyncQueuedDLTx (.-conn fw) out)))))

(defn- deliver-sync-queued-error!
  [^SyncQueuedReq req ^Throwable e]
  (deliver (.-result-promise req) (->SyncQueuedResult nil e)))

(defn- deliver-sync-queued-success!
  [^SyncQueuedReq req report]
  (deliver (.-result-promise req) (->SyncQueuedResult report nil)))

(defn- finalize-sync-queued-report
  [^TxReport report db-after]
  ;; Preserve any extra tx report keys (e.g. :new-attributes) while updating
  ;; db-after to the final shared connection snapshot.
  (assoc report :db-after db-after))

(defn- prepare-sync-queued-batch-reports!
  [conn ^FastList requests ^objects reports]
  (let [n (alength ^objects reports)]
    (loop [i 0
           db ^DB @conn]
      (when (< i n)
        (let [^SyncQueuedReq req (.get requests i)
              ^TxReport report (with db
                                 (.-tx-data req)
                                 (.-tx-meta req)
                                 true)]
          (aset reports i report)
          (recur (inc i) (:db-after report)))))))

(defn- commit-sync-queued-batch-reports!
  [conn ^objects reports]
  (let [n (alength ^objects reports)]
    (with-transaction [c conn]
      (assert (active-conn-structural? c))
      (dotimes [i n]
        (let [^TxReport report (aget reports i)]
          (db/commit-prepared-tx-data! @c (:tx-data report)))))))

(defn- run-sync-queued-dl-batch!
  [conn ^FastList requests]
  (let [n (int (.size requests))]
    (try
      (if (= n 1)
        (let [^SyncQueuedReq req (.get requests 0)]
          (try
            (binding [*sync-queue-worker?* true]
              (let [^TxReport report (-transact! conn
                                                 (.-tx-data req)
                                                 (.-tx-meta req))]
                (deliver-sync-queued-success!
                  req
                  (finalize-sync-queued-report report @conn))))
            (catch Throwable e
              (deliver-sync-queued-error! req e)))
          nil)
        ;; Batch queued strict requests in a single write txn for throughput.
        (let [^objects reports (object-array n)]
          (try
            (binding [*sync-queue-worker?* true]
              ;; Batch preparation allocates concrete entids/tx ids. Keep it
              ;; under the same connection lock as commit so concurrent direct
              ;; writes cannot advance the shared snapshot mid-batch.
              (locking conn
                (prepare-sync-queued-batch-reports! conn requests reports)
                (commit-sync-queued-batch-reports! conn reports)))
            (let [db-after @conn]
              (dotimes [i n]
                (deliver-sync-queued-success!
                  (.get requests i)
                  (finalize-sync-queued-report
                    ^TxReport (aget reports i)
                    db-after))))
            (catch Throwable e
              (dotimes [i n]
                (deliver-sync-queued-error! (.get requests i) e))))
          nil))
      (finally
        (queue-pending-dec-by! conn n)))))

(defn- queued-transact!
  [conn tx-data tx-meta]
  (let [^AtomicLong pending (sync-queue-pending-counter conn)
        ^AtomicLong last-enqueue-ms (sync-queue-last-enqueue-ms-counter conn)
        _ (.set last-enqueue-ms (System/currentTimeMillis))
        _ (.incrementAndGet pending)
        result-promise (promise)
        requests       (doto (FastList. 1)
                         (.add (->SyncQueuedReq tx-data tx-meta result-promise)))]
    (try
      (a/exec-noresult (a/get-executor)
                       (->SyncQueuedDLTx conn requests))
      (catch Throwable e
        ;; Worker did not run, so rollback pending queue count locally.
        (queue-pending-dec-by! conn 1)
        (throw e)))
    (let [^SyncQueuedResult result @result-promise]
      (if-let [e (.-error result)]
        (throw ^Throwable e)
        (let [report (.-report result)]
          (notify-listeners! conn report)
          report)))))

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
   (if (r/dtlv-uri? dir)
     (r/open-kv dir opts)
     (l/open-kv dir opts))))

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
