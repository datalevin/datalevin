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
   [datalevin.async IAsyncWork]
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]))

(defn conn?
  [conn]
  (and (instance? clojure.lang.IDeref conn) (db/db? @conn)))

(defn conn-from-db
  [db]
  {:pre [(db/db? db)]}
  (atom db :meta {:listeners (atom {})
                  :sync-queue-direct-active (AtomicBoolean. false)
                  :sync-queue-pending (AtomicLong. 0)}))

(defn conn-from-datoms
  ([datoms] (conn-from-db (db/init-db datoms)))
  ([datoms dir] (conn-from-db (db/init-db datoms dir)))
  ([datoms dir schema] (conn-from-db (db/init-db datoms dir schema)))
  ([datoms dir schema opts] (conn-from-db (db/init-db datoms dir schema opts))))

(defn create-conn
  ([] (conn-from-db (db/empty-db)))
  ([dir] (conn-from-db (db/empty-db dir)))
  ([dir schema] (conn-from-db (db/empty-db dir schema)))
  ([dir schema opts] (conn-from-db (db/empty-db dir schema opts))))

(defn close
  [conn]
  (when-let [store (.-store ^DB @conn)]
    (i/close ^Store store))
  nil)

(defn closed?
  [conn]
  (or (nil? conn)
      (nil? @conn)
      (i/closed? ^Store (.-store ^DB @conn))))

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
       (locking (l/write-txn s#)
         (db/disable-cache s#)
         (if (instance? DatalogStore s#)
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
                 new-db# (db/new-db s#)]
             (reset! ~orig-conn new-db#)
             (when-not old# (db/enable-cache s#))
             res#)
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
                 new-db# (db/new-db new-s#)]
             (reset! ~orig-conn new-db#)
             (when-not old# (db/enable-cache new-s#))
             res1#))))))

(defn with
  ([db tx-data] (with db tx-data {} false))
  ([db tx-data tx-meta] (with db tx-data tx-meta false))
  ([db tx-data tx-meta simulated?]
   (db/transact-tx-data (db/->TxReport db db [] {} tx-meta)
                        tx-data simulated?)))

(defn db-with
  [db tx-data]
  (:db-after (with db tx-data)))

(defn- -transact! [conn tx-data tx-meta]
  (let [report (with-transaction [c conn]
                 (assert (conn? c))
                 (with @c tx-data tx-meta))]
    (assoc report :db-after @conn)))

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
        direct-active (:sync-queue-direct-active m)
        queued-pending (:sync-queue-pending m)]
    (if (and (instance? AtomicBoolean direct-active)
             (instance? AtomicLong queued-pending))
      {:direct-active direct-active
       :queued-pending queued-pending}
      (let [direct-active (if (instance? AtomicBoolean direct-active)
                            direct-active
                            (AtomicBoolean. false))
            queued-pending (if (instance? AtomicLong queued-pending)
                             queued-pending
                             (AtomicLong. 0))]
        (alter-meta! conn assoc
                     :sync-queue-direct-active direct-active
                     :sync-queue-pending queued-pending)
        {:direct-active direct-active
         :queued-pending queued-pending}))))

(defn- sync-queue-pending-counter
  [conn]
  ^AtomicLong (:queued-pending (ensure-sync-queue-state! conn)))

(defn- queue-pending-dec-by!
  [conn n]
  (let [^AtomicLong pending (sync-queue-pending-counter conn)
        after (.addAndGet pending (- (long n)))]
    (when (neg? after)
      (.set pending 0))))

(defn- begin-relaxed-direct-fast-path!
  [conn]
  (let [{:keys [direct-active queued-pending]} (ensure-sync-queue-state! conn)]
    (when (and (zero? (.get ^AtomicLong queued-pending))
               (.compareAndSet ^AtomicBoolean direct-active false true))
      ^AtomicBoolean direct-active)))

(defn- current-thread-holds-store-write-lock?
  [store]
  (boolean
    (when-let [write-lock (l/write-txn store)]
      (Thread/holdsLock write-lock))))

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
      (alter-meta! conn assoc :remote-store-opts-cache opts)
      opts)))

(defn- txlog-sync-queue-profile
  [conn]
  (when (and (not *sync-queue-worker?*)
             (conn? conn))
    (let [store (.-store ^DB @conn)]
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

          :else nil)))))

(defn- strict-txlog-sync-queue?
  [conn]
  (= :strict (txlog-sync-queue-profile conn)))

(declare queued-transact!)

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
       (do
         (observe-txlog-sync-path! :queued-strict)
         (queued-transact! conn tx-data tx-meta))

       (= :relaxed profile)
       (if-let [^AtomicBoolean direct-active
                (begin-relaxed-direct-fast-path! conn)]
         (try
           (observe-txlog-sync-path! :direct-relaxed)
           (run-transact-now! conn tx-data tx-meta)
           (finally
             (.set direct-active false)))
         (do
           (observe-txlog-sync-path! :queued-relaxed)
           (queued-transact! conn tx-data tx-meta)))

       :else
       (do
         (observe-txlog-sync-path! :queued-other)
         (queued-transact! conn tx-data tx-meta))))))

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
      (alter-meta! conn assoc
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

(defn- add-conn [dir conn] (swap! connections assoc dir conn))

(defn- new-conn
  [dir schema opts]
  (let [conn (create-conn dir schema opts)]
    (add-conn dir conn)
    conn))

(defn get-conn
  ([dir]
   (get-conn dir nil nil))
  ([dir schema]
   (get-conn dir schema nil))
  ([dir schema opts]
   (if-let [c (get @connections dir)]
     (if (closed? c) (new-conn dir schema opts) c)
     (new-conn dir schema opts))))

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
         sync-queued-dl-tx-combine
         run-sync-queued-dl-batch!)

(defn- dl-work-key* [db-name] (->> db-name hash (str "tx") keyword))

(def ^:no-doc dl-work-key (memoize dl-work-key*))
(defn- sync-queued-dl-work-key* [db-name] (->> db-name hash (str "tx-sync") keyword))
(def ^:private sync-queued-dl-work-key (memoize sync-queued-dl-work-key*))

(deftype ^:no-doc SyncQueuedReq [tx-data tx-meta result-promise])
(deftype ^:no-doc SyncQueuedResult [report error])

(deftype ^:no-doc AsyncDLTx [conn store tx-data tx-meta cb]
  IAsyncWork
  (work-key [_] (->> (.-store ^DB @conn) i/db-name dl-work-key))
  (do-work [_] (run-transact-now! conn tx-data tx-meta))
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
                   (.-store fw)
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

(defn- run-sync-queued-dl-batch!
  [conn ^FastList requests]
  (let [n (int (.size requests))]
    (try
      (if (= n 1)
        (let [^SyncQueuedReq req (.get requests 0)]
          (try
            (let [report-v (volatile! nil)]
              (binding [*sync-queue-worker?* true]
                (with-transaction [c conn]
                  (assert (conn? c))
                  (let [^TxReport report (with @c (.-tx-data req) (.-tx-meta req))]
                    (reset! c (:db-after report))
                    (vreset! report-v report))))
              (deliver-sync-queued-success!
                req
                (finalize-sync-queued-report ^TxReport @report-v @conn)))
            (catch Throwable e
              (deliver-sync-queued-error! req e)))
          nil)
        (let [reports (object-array n)]
          (try
            (binding [*sync-queue-worker?* true]
              (with-transaction [c conn]
                (assert (conn? c))
                (dotimes [i n]
                  (let [^SyncQueuedReq req (.get requests i)
                        ^TxReport report (with @c (.-tx-data req) (.-tx-meta req))]
                    (reset! c (:db-after report))
                    (aset reports i report)))))
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
   (a/exec (a/get-executor)
           (let [store (.-store ^DB @conn)]
             (if (instance? DatalogStore store)
               (->AsyncDLTx conn store tx-data tx-meta callback)
               (let [lmdb (.-lmdb ^Store store)]
                 (->AsyncDLTx conn lmdb tx-data tx-meta callback)))))))

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
      (doseq [dbi [c/eav c/ave c/giants c/schema c/meta]]
        (i/clear-dbi lmdb dbi))
      (finally
        (db/remove-cache store)
        (i/close-kv lmdb)))))
