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
   [datalevin.validate :as vld]
   [datalevin.index :as idx])
  (:import
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.remote DatalogStore]
   [datalevin.async IAsyncWork]))

(defn conn?
  [conn]
  (and (instance? clojure.lang.IDeref conn) (db/db? @conn)))

(defn conn-from-db
  [db]
  {:pre [(db/db? db)]}
  (atom db :meta { :listeners (atom {}) }))

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

(def ^:dynamic *explicit-transaction?* false)

(defmacro ^:no-doc with-transaction*
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
                 db1#    (volatile! db#)
                 res1#   (l/with-transaction-kv [kv1# kv#]
                           (let [conn1# (atom (db/transfer
                                                db# (s/transfer s# kv1#))
                                              :meta (meta ~orig-conn))
                                 res#   (let [~conn conn1#]
                                          ~@body)
                                 db2#   (if (and (map? res#)
                                                 (instance? DB (:db-after res#)))
                                          ^DB (:db-after res#)
                                          ^DB (deref conn1#))]
                             (vreset! db1# db2#)
                             (vreset! s1# (.-store db2#))
                             res#))
                 new-s#  (s/transfer (deref s1#) kv#)
                 new-db# (db/transfer ^DB (deref db1#) new-s#)]
             (reset! ~orig-conn new-db#)
             (when-not old# (db/enable-cache new-s#))
             res1#))))))

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
  `(binding [*explicit-transaction?* true]
     (with-transaction* [~conn ~orig-conn] ~@body)))

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
  (let [report (with-transaction* [c conn]
                 (assert (conn? c))
                 (with @c tx-data tx-meta))]
    (cond-> (assoc report :db-after @conn)
      *explicit-transaction?* (assoc :tx-provisional? true))))

(defn transact!
  ([conn tx-data] (transact! conn tx-data nil))
  ([conn tx-data tx-meta]
   (let [report (-transact! conn tx-data tx-meta)]
     (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
       (callback report))
     report)))

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
  (i/opts ^Store (.-store ^DB @conn)))

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
   (let [^DB db       (db conn)
         ^Store store (.-store db)
         current-schema (i/schema store)
         rename-entries (seq rename-map)
         projected-schema (let [s (merge current-schema (or schema-update {}))]
                            (if (seq del-attrs)
                              (apply dissoc s del-attrs)
                              s))]
     (when schema-update
       (if (instance? Store store)
         (vld/validate-schema-update
           store (.-lmdb ^Store store) schema-update
           {:fulltext (set (keys (.-search-engines ^Store store)))
            :vector   (set (keys (.-vector-indices ^Store store)))
            :idoc     (set (keys (.-idoc-indices ^Store store)))})
         (vld/validate-schema schema-update)))
     (doseq [attr del-attrs]
       (vld/validate-del-attr store attr))
     (vld/validate-rename-map projected-schema rename-entries)
     ;; Migrate untyped → typed attributes before schema is applied
     (when (and schema-update (instance? Store store))
       (doseq [[attr new-props] schema-update
               :let [old-props (current-schema attr)]
               :when old-props
               :let [old-vt (idx/value-type old-props)
                     new-vt (idx/value-type new-props)]
               :when (and (identical? old-vt :data)
                          (not (identical? new-vt :data)))]
         (s/migrate-attr-values store attr new-vt)))
     (i/set-schema store schema-update)
     (doseq [attr del-attrs]
       (i/del-attr store attr))
     (doseq [[old new] rename-entries]
       (i/rename-attr store old new))
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

(declare dl-tx-combine)

(defn- dl-work-key* [db-name] (->> db-name hash (str "tx") keyword))

(def ^:no-doc dl-work-key (memoize dl-work-key*))

(deftype ^:no-doc AsyncDLTx [conn store tx-data tx-meta cb]
  IAsyncWork
  (work-key [_] (->> (.-store ^DB @conn) i/db-name dl-work-key))
  (do-work [_] (transact! conn tx-data tx-meta))
  (combine [_] dl-tx-combine)
  (callback [_] cb))

(defn- dl-tx-combine
  [coll]
  (let [^AsyncDLTx fw (first coll)]
    (->AsyncDLTx (.-conn fw)
                 (.-store fw)
                 (into [] (comp (map #(.-tx-data ^AsyncDLTx %)) cat) coll)
                 (.-tx-meta fw)
                 (.-cb fw))))

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
        remote? (instance? DatalogStore store)
        lmdb  (if remote?
                (let [dir (i/dir store)]
                  (close conn)
                  (open-kv dir))
                (.-lmdb ^Store store))
        dir   (when-not remote?
                (i/env-dir lmdb))
        wal?  (if remote?
                false
                (boolean (:kv-wal? (i/env-opts lmdb))))]
    (try
      ;; For remote stores, force WAL catch-up before clearing base DBIs.
      ;; Otherwise reopening can replay historical WAL and resurrect data.
      (when remote?
        (i/flush-kv-indexer! lmdb))
      (binding [c/*bypass-wal* true]
        (doseq [dbi [c/eav c/ave c/giants c/schema c/meta]]
          (i/clear-dbi lmdb dbi)))
      ;; After a full clear, historical WAL is irrelevant. Reset replay markers
      ;; in kv-info directly and drop WAL files to avoid replaying stale history
      ;; when reopening this directory.
      (when wal?
        (binding [c/*bypass-wal* true]
          (i/transact-kv lmdb c/kv-info
                         [[:put c/last-indexed-wal-tx-id 0]
                          [:put c/applied-wal-tx-id 0]
                          [:put c/legacy-applied-tx-id 0]]
                         :data :data)))
      (finally
        (db/remove-cache store)
        (i/close-kv lmdb)
        (when wal?
          (u/delete-files (str dir u/+separator+ c/wal-dir)))))))
