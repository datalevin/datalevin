;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server
  "Non-blocking event-driven database server with role based access control"
  (:refer-clojure :exclude [run-calls sync])
  (:require
   [datalevin.util :as u]
   [datalevin.core :as d]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.db :as db]
   [datalevin.udf :as udf]
   [datalevin.lmdb :as l]
   [datalevin.binding.cpp :as cpp]
   [datalevin.protocol :as p]
   [datalevin.storage :as st]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.replication :as drep]
   [datalevin.ha.util :as hu]
   [datalevin.server.auth :as auth]
   [datalevin.server.copy :as scopy]
   [datalevin.server.dispatch :as sdisp]
   [datalevin.server.handlers :as sh]
   [datalevin.server.ha :as sha]
   [datalevin.server.session :as sess]
   [datalevin.txlog :as txlog]
   [datalevin.kv :as kv]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [taoensso.timbre :as log]
   [clojure.stacktrace :as stt]
   [clojure.string :as s])
  (:import
   [java.nio ByteBuffer BufferOverflowException]
   [java.nio.file Files Paths OpenOption]
   [java.nio.channels ClosedChannelException Selector SelectionKey
    ServerSocketChannel SocketChannel]
   [java.net InetSocketAddress]
   [java.security MessageDigest]
   [java.util Iterator UUID Map]
   [java.util.function BiFunction]
   [java.util.concurrent.atomic AtomicBoolean]
   [java.util.concurrent Executors Executor ExecutorService Future
    ConcurrentLinkedQueue ConcurrentHashMap CountDownLatch Semaphore TimeUnit
    LinkedBlockingQueue]
   [java.util.concurrent.locks ReentrantReadWriteLock]
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.interface ILMDB IStore]))

(defprotocol IServer
  (start [srv] "Start the server")
  (stop [srv] "Stop the server"))

;; system db management

(def server-schema auth/server-schema)

(def permission-actions auth/permission-actions)

(def permission-objects auth/permission-objects)

(def salt auth/salt)

(def password-hashing auth/password-hashing)

(def password-matches? auth/password-matches?)

(def ^:private pull-user auth/pull-user)
(def ^:private query-user auth/query-user)
(def ^:private pull-db auth/pull-db)
(def ^:private query-role auth/query-role)
(def ^:private user-eid auth/user-eid)
(def ^:private db-eid auth/db-eid)
(def ^:private role-eid auth/role-eid)
(def ^:private eid->username auth/eid->username)
(def ^:private eid->db-name auth/eid->db-name)
(def ^:private eid->role-key auth/eid->role-key)
(def ^:private query-users auth/query-users)
(def ^:private user-roles auth/user-roles)
(def ^:private query-roles auth/query-roles)
(def ^:private perm-tgt-eid auth/perm-tgt-eid)
(def ^:private perm-tgt-name auth/perm-tgt-name)
(def ^:private user-permissions auth/user-permissions)
(def ^:private role-permissions auth/role-permissions)
(def ^:private user-role-eid auth/user-role-eid)
(def ^:private permission-eid auth/permission-eid)
(def ^:private role-permission-eid auth/role-permission-eid)
(def ^:private query-databases auth/query-databases)
(def ^:private user-role-key auth/user-role-key)
(def ^:private user-role-key? auth/user-role-key?)
(def ^:private transact-new-user auth/transact-new-user)
(def ^:private transact-new-password auth/transact-new-password)
(def ^:private transact-drop-user auth/transact-drop-user)
(def ^:private transact-new-role auth/transact-new-role)
(def ^:private transact-drop-role auth/transact-drop-role)
(def ^:private transact-user-role auth/transact-user-role)
(def ^:private transact-withdraw-role auth/transact-withdraw-role)
(def ^:private transact-role-permission auth/transact-role-permission)
(def ^:private transact-revoke-permission auth/transact-revoke-permission)
(def ^:private transact-new-db auth/transact-new-db)
(def ^:private transact-drop-db auth/transact-drop-db)

(defn- close-store
  [store]
  (cond
    (instance? IStore store) (i/close store)
    (instance? ILMDB store)  (i/close-kv store)
    :else                    (u/raise "Unknown store" {})))

(declare store-closed?)

(defn- reopen-store
  [store]
  (cond
    (instance? IStore store)
    (let [env-dir (i/dir store)]
      (when-not (store-closed? store)
        (try
          (close-store store)
          (catch Throwable _
            nil)))
      (dha/recover-ha-local-store-dir-if-needed! env-dir)
      (st/open env-dir (i/schema store) (i/opts store)))

    (instance? ILMDB store)
    (let [env-dir (i/env-dir store)
          env-opts (i/env-opts store)]
      (when-not (store-closed? store)
        (try
          (close-store store)
          (catch Throwable _
            nil)))
      (l/open-kv env-dir env-opts))

    :else
    (u/raise "Unknown store" {})))

(defn- closed-store-race?
  [t store]
  (or (try
        (store-closed? store)
        (catch Throwable _
          true))
      (and t
           (s/includes? (or (ex-message t) "")
                        "LMDB env is closed"))))

(defn- transient-write-open-race?
  [t store]
  (or (closed-store-race? t store)
      (and t
           (instance? Store store)
           (s/includes? (or (ex-message t) "")
                        "Invalid argument"))))

(declare get-store get-kv-store add-store)

(defn- open-write-txn-with-retry
  [server db-name]
  (loop [attempt 0]
    (let [store (get-store server db-name)
          kv-store (get-kv-store server db-name)
          tx-lock (l/write-txn kv-store)
          result (locking kv-store
                   ;; Keep explicit write-txn opens on the same LMDB monitor as
                   ;; txlog floor bookkeeping and `with-transaction-kv` so
                   ;; background one-shot writes cannot race shared write state.
                   (locking tx-lock
                     (try
                       {:ok? true
                        :store store
                        :kv-store kv-store
                        :wlmdb (i/open-transact-kv kv-store)}
                       (catch Throwable t
                         {:ok? false
                          :store store
                          :error t}))))]
      (if (:ok? result)
        result
        (let [t (:error result)]
          (if (and (zero? attempt)
                   (transient-write-open-race? t store))
            (do
              (add-store server db-name (reopen-store store))
              (recur (inc attempt)))
            (throw t)))))))

(def ^:private has-permission? auth/has-permission?)

(defmacro wrap-permission
  [req-act req-obj req-tgt message & body]
  `(let [{:keys [~'client-id ~'write-bf ~'wire-opts]} @(~'.attachment ~'skey)
         ~'ch                                         (~'.channel ~'skey)
         {:keys [~'permissions]}          (get-client ~'server ~'client-id)]
     (if ~'permissions
       (if (has-permission? ~req-act ~req-obj ~req-tgt ~'permissions)
         (do ~@body)
         (u/raise ~message {}))
       (do
         (remove-client ~'server ~'client-id)
         (p/write-message-blocking ~'ch ~'write-bf
                                   {:type :reconnect}
                                   ~'wire-opts)))))

(declare event-loop close-conn store->db-name session-lmdb remove-store
         halt-run session-deps copy-deps dispatch-deps ha-deps)

(def session-dbi sess/session-dbi)

(defn- shutdown-executor!
  [^ExecutorService es label]
  (.shutdown es)
  (when-not (.awaitTermination es 5000 TimeUnit/MILLISECONDS)
    (log/warn label "did not terminate in 5s, forcing shutdown")
    (.shutdownNow es)
    (when-not (.awaitTermination es 5000 TimeUnit/MILLISECONDS)
      (log/warn label "did not terminate after forced shutdown"))))

(deftype Server [^AtomicBoolean running
                 ^int port
                 ^String root
                 ^long idle-timeout
                 ^ServerSocketChannel server-socket
                 ^Selector selector
                 ^ConcurrentLinkedQueue register-queue
                 ^ExecutorService dispatcher
                 ^ExecutorService work-executor
                 sys-conn
                 ;; client session data, a map of
                 ;; client-id -> { ip, uid, username, roles, permissions,
                 ;;                last-active,
                 ;;                stores -> { db-name -> {datalog?
                 ;;                                        dbis -> #{dbi-name}}}
                 ;;                engines -> #{ db-name }
                 ;;                indices -> #{ db-name }
                 ;;                dt-dbs -> #{ db-name } }
                 ^ConcurrentHashMap clients
                 ;; db state data, a map of
                 ;; db-name -> { store, search engine, vector index,
                 ;;              datalog db, lock, write txn runner,
                 ;;              and writing variants of stores }
                 dbs]
  IServer
  (start [server]
    (letfn [(init []
              (log/info "Datalevin server started on port" port)
              (try (event-loop server)
                   (catch Exception e
                     (when (.get running)
                       (.submit dispatcher ^Callable init)))))]
      (when-not (.get running)
        (.set running true)
        (try
          (.submit dispatcher ^Callable init)
          (catch Throwable t
            (.set running false)
            (throw t))))))

  (stop [server]
    (.set running false)
    (.wakeup selector)
    (doseq [skey (.keys selector)] (close-conn skey))
    (.close server-socket)
    (when (.isOpen selector) (.close selector))
    (shutdown-executor! dispatcher "Server dispatcher")
    (shutdown-executor! work-executor "Server worker executor")
    (doseq [db-name (keys dbs)] (remove-store server db-name))
    (d/close sys-conn)
    (log/info "Datalevin server shuts down.")))

(defn- get-client [^Server server client-id]
  (sess/get-client (.-clients server) client-id))

(defn- add-client
  [^Server server ip client-id username]
  (sess/add-client (session-deps) server ip client-id username))

(defn- remove-client
  [^Server server client-id]
  (sess/remove-client (session-deps) server client-id))

(defn- update-client
  [^Server server client-id f]
  (sess/update-client (session-deps) server client-id f))

(declare get-store store-closed?)
(declare current-runtime-opts new-runtime-db)

(defn- usable-store
  [store]
  (when-not
    (try
      (cond
        (nil? store) true
        (instance? IStore store) (i/closed? store)
        (instance? ILMDB store) (i/closed-kv? store)
        :else true)
      (catch Throwable _
        true))
    store))

(defn- runtime-db-store
  [dt-db]
  (when (instance? DB dt-db)
    (usable-store (.-store ^DB dt-db))))

(defn- get-stores
  [^Server server]
  (into {}
        (keep (fn [[db-name _]]
                (when-let [store (get-store server db-name)]
                  [db-name store])))
        (.-dbs server)))

(defn- get-store
  ([^Server server db-name writing?]
   (let [m (get (.-dbs server) db-name)]
     (if writing?
       (or (usable-store (:wstore m))
           (runtime-db-store (:wdt-db m)))
       (or (usable-store (:store m))
           (runtime-db-store (:dt-db m))))))
  ([server db-name]
   (get-store server db-name false)))

(defn- update-db
  [^Server server db-name f]
  (let [^ConcurrentHashMap dbs (.-dbs server)
        new-v                 (volatile! nil)]
    (.compute dbs db-name
              (reify BiFunction
                (apply [_ _ old]
                  (let [new (f (or old {}))]
                    (vreset! new-v new)
                    new))))
    @new-v))

(defn- replace-db-state-if-current
  [^Server server db-name expected-state guard-fn new-state]
  (let [^ConcurrentHashMap dbs (.-dbs server)
        present? (volatile! false)
        updated? (volatile! false)
        final-v  (volatile! nil)]
    (.computeIfPresent dbs db-name
                       (reify BiFunction
                         (apply [_ _ state]
                           (vreset! present? true)
                           (let [next (if (and (identical? state expected-state)
                                               (guard-fn state))
                                        (do
                                          (vreset! updated? true)
                                          new-state)
                                        state)]
                             (vreset! final-v next)
                             next))))
    {:updated? @updated?
     :state (when @present? @final-v)}))

(defn- transform-db-state-when
  [^Server server db-name guard-fn f]
  (let [^ConcurrentHashMap dbs (.-dbs server)
        present? (volatile! false)
        updated? (volatile! false)
        final-v  (volatile! nil)]
    (.computeIfPresent dbs db-name
                       (reify BiFunction
                         (apply [_ _ state]
                           (vreset! present? true)
                           (let [next (if (guard-fn state)
                                        (do
                                          (vreset! updated? true)
                                          (f state))
                                        state)]
                             (vreset! final-v next)
                             next))))
    {:updated? @updated?
     :state (when @present? @final-v)}))

(def ^:private missing-state-value sha/missing-state-value)
(def ^:private ha-follower-local-side-effect-keys
  sha/ha-follower-local-side-effect-keys)
(def ^:private ha-follower-side-effect-keys
  sha/ha-follower-side-effect-keys)
(def ^:private state-patch sha/state-patch)
(def ^:private ha-follower-local-side-effect-patch
  sha/ha-follower-local-side-effect-patch)
(def ^:private ha-follower-side-effect-patch
  sha/ha-follower-side-effect-patch)
(def ^:private ha-renew-merge-excluded-keys
  sha/ha-renew-merge-excluded-keys)
(def ^:private ha-renew-state-patch sha/ha-renew-state-patch)
(def ^:private apply-state-patch sha/apply-state-patch)
(def ^:private same-ha-runtime-context? sha/same-ha-runtime-context?)
(def ^:private same-ha-runtime-state? sha/same-ha-runtime-state?)
(def ^:private merge-ha-follower-local-side-effect-patch
  sha/merge-ha-follower-local-side-effect-patch)
(def ^:private merge-ha-follower-side-effect-patch
  sha/merge-ha-follower-side-effect-patch)
(def ^:private merge-ha-renew-state-patch
  sha/merge-ha-renew-state-patch)
(def ^:private merge-ha-renew-promotion-state-patch
  sha/merge-ha-renew-promotion-state-patch)
(def ^:private persist-ha-follower-side-effects!
  sha/persist-ha-follower-side-effects!)

(def ^:dynamic *server-runtime-opts-fn*
  (fn [_ _ _ _] nil))

(defn- current-runtime-opts
  [m]
  (or (:runtime-opts m)
      (some-> (:dt-db m) db/runtime-opts)
      (some-> (:wdt-db m) db/runtime-opts)
      {}))

(defn- resolved-runtime-opts
  [server db-name store m]
  (let [current  (current-runtime-opts m)
        resolved (*server-runtime-opts-fn* server db-name store m)]
    (cond
      (and (map? current) (map? resolved))
      (merge current resolved)

      (map? resolved)
      resolved

      :else
      current)))

(defn- attach-runtime-opts
  [dt-db runtime-opts]
  (cond-> dt-db
    (seq runtime-opts) (db/with-runtime-opts runtime-opts)))

(defn- fresh-runtime-db-info
  [store]
  (when (instance? Store store)
    (let [lmdb          (kv/raw-lmdb (.-lmdb ^Store store))
          max-tx        (long (or (i/get-value lmdb c/meta :max-tx :attr :long)
                                  (i/max-tx store)))
          last-modified (long (or (i/get-value lmdb c/meta :last-modified
                                               :attr :long)
                                  (i/last-modified store)
                                  (System/currentTimeMillis)))]
      {:max-eid       (i/init-max-eid store)
       :max-tx        max-tx
       :last-modified last-modified})))

(defn- new-runtime-db
  [store runtime-opts]
  (attach-runtime-opts (db/new-db store (fresh-runtime-db-info store))
                       runtime-opts))

(def ^:private installed-udf-query
  '[:find ?ident ?descriptor
    :where
    [?e :db/ident ?ident]
    [?e :db/udf ?descriptor]])

(defn- udf-readiness-required?
  [m]
  (true? (:ha-require-udf-ready? (current-runtime-opts m))))

(defn- udf-readiness-token
  [m dt-db]
  [(db/udf-cache-token dt-db)
   (long (or (:max-tx dt-db)
             (some-> (:store m) i/max-tx)
             0))])

(defn- installed-tx-udfs
  [dt-db]
  (keep
    (fn [[ident descriptor]]
      (let [descriptor (udf/descriptor descriptor)]
        (when (= :tx-fn (:udf/kind descriptor))
          {:db/ident ident
           :descriptor descriptor})))
    (d/q installed-udf-query dt-db)))

(defn- compute-udf-readiness
  [m dt-db]
  (let [registry (db/udf-registry dt-db)
        context  {:db        dt-db
                  :kind      :tx-fn
                  :embedded? true
                  :store     (:store m)}
        missing  (reduce
                   (fn [acc {:keys [db/ident descriptor]}]
                     (try
                       (udf/materialize registry context descriptor)
                       acc
                       (catch Throwable t
                         (conj acc {:db/ident   ident
                                    :descriptor descriptor
                                    :error      (or (:error (ex-data t))
                                                    :udf/not-found)}))))
                   []
                   (installed-tx-udfs dt-db))]
    {:udf-ready? false
     :udf-missing missing}))

(defn- ensure-udf-readiness-state
  [m]
  (if-not (udf-readiness-required? m)
    m
    (let [runtime-opts (current-runtime-opts m)
          dt-db        (or (:dt-db m)
                           (when-let [store (:store m)]
                             (new-runtime-db store runtime-opts)))]
      (if-not dt-db
        m
        (let [token (udf-readiness-token m dt-db)]
          (if (= token (:udf-readiness-token m))
            (cond-> m
              (nil? (:dt-db m)) (assoc :dt-db dt-db))
            (let [{:keys [udf-ready? udf-missing]}
                  (let [result (compute-udf-readiness m dt-db)]
                    (if (empty? (:udf-missing result))
                      {:udf-ready? true :udf-missing []}
                      result))]
              (cond-> (assoc m
                             :udf-ready? udf-ready?
                             :udf-missing udf-missing
                             :udf-readiness-token token)
                (nil? (:dt-db m)) (assoc :dt-db dt-db)))))))))

(def ^:dynamic *ensure-udf-readiness-state-fn*
  ensure-udf-readiness-state)

(defn- udf-write-admission-error
  [db-name m]
  (when (and (:ha-authority m)
             (= :leader (:ha-role m))
             (udf-readiness-required? m)
             (false? (:udf-ready? m)))
    (let [owner-node-id (:ha-authority-owner-node-id m)
          owner-endpoint (or (get-in m [:ha-authority-lease :leader-endpoint])
                             (some->> (:ha-members m)
                                      (filter #(= owner-node-id
                                                  (:node-id %)))
                                      first
                                      :endpoint))
          ordered-endpoints
          (into []
                (comp
                 (map :endpoint)
                 (remove nil?)
                 (remove s/blank?))
                (sort-by :node-id (:ha-members m)))
          retry-endpoints
          (->> (cond-> []
                 (and (string? owner-endpoint)
                      (not (s/blank? owner-endpoint)))
                 (conj owner-endpoint)
                 :always
                 (into ordered-endpoints))
               distinct
               vec)]
      {:error                        :ha/write-rejected
       :reason                       :udf-not-ready
       :retryable?                   false
       :db-name                      db-name
       :ha-role                      (:ha-role m)
       :ha-retry-endpoints           retry-endpoints
       :ha-authoritative-leader-endpoint owner-endpoint
       :ha-authoritative-leader-node-id owner-node-id
       :udf-missing                  (:udf-missing m)})))

(def ^:private udf-admission-exempt-write-types
  #{:txlog-update-snapshot-floor!
    :txlog-clear-snapshot-floor!
    :txlog-update-replica-floor!
    :txlog-clear-replica-floor!
    :txlog-pin-backup-floor!
    :txlog-unpin-backup-floor!})

(def consensus-ha-opts sha/consensus-ha-opts)

(def ^:dynamic *consensus-ha-opts-fn*
  consensus-ha-opts)

(def ^:private ha-runtime-option-keys sha/ha-runtime-option-keys)
(def ^:private ha-runtime-option-key-set sha/ha-runtime-option-key-set)
(def ^:private sanitize-ha-path-segment sha/sanitize-ha-path-segment)
(def ^:private default-ha-control-raft-dir sha/default-ha-control-raft-dir)
(def ^:private with-default-ha-control-raft-dir
  sha/with-default-ha-control-raft-dir)
(def ^:private start-ha-authority sha/start-ha-authority)
(def ^:private stop-ha-authority sha/stop-ha-authority)

(def ^:dynamic *ha-renew-step-fn*
  dha/ha-renew-step)

(def ^:dynamic *ha-follower-sync-step-fn*
  dha/ha-follower-sync-step)

(defn- ha-renew-step
  [db-name m]
  (*ha-renew-step-fn* db-name m))

(defn- ha-follower-sync-step
  [db-name m]
  (*ha-follower-sync-step-fn* db-name m))

(declare get-lock
         db-write-admission-lock
         with-db-runtime-store-read-access
         with-db-runtime-store-swap)

(defn- ha-follower-apply-record-with-guard
  [^Server server db-name expected-state record]
  (sha/ha-follower-apply-record-with-guard
   (ha-deps) server db-name expected-state record))

(defn- with-ha-follower-replay-quiesced
  [^Server server db-name f]
  (sha/with-ha-follower-replay-quiesced (ha-deps) server db-name f))

(def ^:private ha-loop-sleep-ms sha/ha-loop-sleep-ms)
(def ^:private ha-follower-loop-sleep-ms sha/ha-follower-loop-sleep-ms)
(def ^:private sleep-ha-loop! sha/sleep-ha-loop!)
(def ^:private ha-loop-error-backoff! sha/ha-loop-error-backoff!)

(defn- ha-renew-promotion-result?
  [expected-state next-state]
  (and (not (contains? #{:leader :demoting} (:ha-role expected-state)))
       (contains? #{:leader :demoting} (:ha-role next-state))
       (= (:ha-node-id next-state)
          (:ha-authority-owner-node-id next-state))))

(defn- publish-ha-renew-state!
  [^Server server db-name expected-state next-state ^AtomicBoolean running?]
  (sha/publish-ha-renew-state!
   (ha-deps) server db-name expected-state next-state running?))

(declare log-ha-loop-crash!)

(defn- run-ha-renew-loop
  [^Server server db-name ^AtomicBoolean running? ^CountDownLatch stopped-latch]
  (sha/run-ha-renew-loop (ha-deps) server db-name running? stopped-latch))

(defn- run-ha-follower-sync-loop
  [^Server server db-name ^AtomicBoolean running? ^CountDownLatch stopped-latch]
  (sha/run-ha-follower-sync-loop
   (ha-deps) server db-name running? stopped-latch))

(declare execute)

(defn- ensure-ha-renew-loop
  [^Server server db-name]
  (sha/ensure-ha-renew-loop (ha-deps) server db-name))

(defn- ensure-ha-follower-sync-loop
  [^Server server db-name]
  (sha/ensure-ha-follower-sync-loop (ha-deps) server db-name))

(defn- stop-ha-renew-loop
  [m]
  (sha/stop-ha-renew-loop m))

(defn- stop-ha-follower-sync-loop
  [m]
  (sha/stop-ha-follower-sync-loop m))

(def ^:private await-ha-loop-stop sha/await-ha-loop-stop)

(def ^:dynamic *start-ha-authority-fn*
  start-ha-authority)

(def ^:dynamic *stop-ha-authority-fn*
  stop-ha-authority)

(def ^:dynamic *stop-ha-renew-loop-fn*
  stop-ha-renew-loop)

(def ^:dynamic *stop-ha-follower-sync-loop-fn*
  stop-ha-follower-sync-loop)

(defn- current-ha-runtime-local-opts
  [m]
  (sha/current-ha-runtime-local-opts (ha-deps) m))

(defn- resolved-ha-runtime-opts
  ([root db-name store]
   (resolved-ha-runtime-opts root db-name store nil nil))
  ([root db-name store m]
   (resolved-ha-runtime-opts root db-name store m nil))
  ([root db-name store m explicit-ha-runtime-opts]
   (sha/resolved-ha-runtime-opts
    (ha-deps) root db-name store m explicit-ha-runtime-opts)))

(def ^:private shared-store-lifecycle? sha/shared-store-lifecycle?)

(defn- stop-ha-runtime
  [db-name m]
  (sha/stop-ha-runtime (ha-deps) db-name m))

(def ^:private ha-authority-running? sha/ha-authority-running?)

(declare db-write-admission-lock)

(defn- ha-write-admission-error
  [^Server server message]
  (sha/ha-write-admission-error (ha-deps) server message))

(defn- leader-authority-state?
  [m]
  (and (= :leader (:ha-role m))
       (satisfies? ctrl/ILeaseAuthority (:ha-authority m))))

(defn- refresh-ha-write-commit-state!
  [^Server server db-name]
  (sha/refresh-ha-write-commit-state! (ha-deps) server db-name))

(defn- ha-write-commit-admission!
  [^Server server message]
  (sha/ha-write-commit-admission! (ha-deps) server message))

(defn- ha-write-commit-check-fn
  [^Server server message]
  (sha/ha-write-commit-check-fn (ha-deps) server message))

(defn- with-ha-write-admission
  [^Server server message f]
  (sha/with-ha-write-admission (ha-deps) server message f))

(def ^:private ha-abort-cleanup-types
  #{:abort-transact
    :abort-transact-kv})

(def ^:private ha-rejected-close-cleanup-types
  #{:close-transact
    :close-transact-kv})

(defn- cleanup-rejected-close-transact!
  [^Server server {:keys [type args]}]
  (sha/cleanup-rejected-close-transact! (ha-deps) server {:type type :args args}))

(defn- ensure-ha-runtime
  ([root db-name m store]
   (ensure-ha-runtime root db-name m store nil))
  ([root db-name m store explicit-ha-runtime-opts]
   (sha/ensure-ha-runtime
    (ha-deps) root db-name m store explicit-ha-runtime-opts)))

(defn- add-store
  ([server db-name store]
   (add-store server db-name store true nil))
  ([^Server server db-name store activate-runtime?]
   (add-store server db-name store activate-runtime? nil))
  ([^Server server db-name store activate-runtime? explicit-ha-runtime-opts]
   (letfn [(add-store* [store]
             (let [published-store-v (volatile! store)
                   close-unpublished-store!
                   (fn []
                     (let [published-store @published-store-v]
                       (when (and (some? published-store)
                                  (not (shared-store-lifecycle?
                                         published-store
                                         store))
                                  (not (store-closed? published-store)))
                         (close-store published-store))
                       (when (and (some? store)
                                  (not (identical? published-store store))
                                  (not (shared-store-lifecycle?
                                         store
                                         published-store))
                                  (not (store-closed? store)))
                         (close-store store))))]
               (try
                 (update-db
                   server db-name
                   (fn [m]
                     (let [dt-db ^DB (:dt-db m)
                           runtime-store
                           (when (instance? DB dt-db)
                             (.-store dt-db))
                           published-store
                           (if (and (not activate-runtime?)
                                    (some? runtime-store)
                                    (not (store-closed? runtime-store))
                                    (not (identical? runtime-store store)))
                             runtime-store
                             store)
                           published-store
                           (dha/recover-ha-local-store-if-needed
                            published-store)
                           ha-runtime-opts
                           (resolved-ha-runtime-opts
                            (.-root server)
                            db-name
                            published-store
                            m
                            explicit-ha-runtime-opts)
                           _            (vreset! published-store-v
                                                 published-store)
                           runtime-opts (resolved-runtime-opts
                                          server db-name published-store m)
                           runtime-local-opts
                           (some-> ha-runtime-opts
                                   dha/select-ha-runtime-local-opts)
                           next-m       (assoc m
                                               :store published-store
                                               :runtime-opts runtime-opts)
                           next-m       (cond-> next-m
                                          (and (not activate-runtime?)
                                               (some? ha-runtime-opts))
                                          (assoc :ha-runtime-opts
                                                 ha-runtime-opts
                                                 :ha-runtime-local-opts
                                                 runtime-local-opts)

                                          (and (not activate-runtime?)
                                               (nil? ha-runtime-opts))
                                          (dissoc :ha-runtime-opts
                                                  :ha-runtime-local-opts))
                           next-m       (cond-> next-m
                                          (and activate-runtime?
                                               (instance? IStore
                                                          published-store))
                                          (assoc :dt-db
                                                 (new-runtime-db
                                                   published-store
                                                   runtime-opts)))]
                       (if activate-runtime?
                         (ensure-ha-runtime
                           (.-root server)
                           db-name
                           next-m
                           published-store
                           explicit-ha-runtime-opts)
                         next-m))))
                 (when (and (not (shared-store-lifecycle?
                                  @published-store-v
                                  store))
                            (not (store-closed? store)))
                   (close-store store))
                 (ensure-ha-renew-loop server db-name)
                 (ensure-ha-follower-sync-loop server db-name)
                 @published-store-v
                 (catch Throwable t
                   (close-unpublished-store!)
                   (throw t)))))
          (attempt-add-store [store ^long retries]
            (try
              (add-store* store)
              (catch Throwable t
                (if (and (pos? retries)
                         (closed-store-race? t store))
                  (do
                    (Thread/sleep 50)
                    (attempt-add-store (reopen-store store)
                                       (unchecked-dec retries)))
                  (throw t)))))]
     (attempt-add-store store 3))))

(defn- get-db
  ([server db-name]
   (get-db server db-name false))
  ([^Server server db-name writing?]
   (let [m (get (.-dbs server) db-name)]
     (if writing?
       (:wdt-db m)
       (or
       (when (or (:ha-authority m)
                 (:ha-role m))
           (when-let [store (or (usable-store (:store m))
                                (runtime-db-store (:dt-db m)))]
            (cpp/invalidate-thread-reader!
             (kv/raw-lmdb
              (if (instance? Store store)
                (.-lmdb ^Store store)
                store)))
            ;; HA replay and promotion mutate the shared store outside the
            ;; normal query/transaction wrappers. Clear the shared store cache
            ;; and build a fresh runtime DB view for HA reads whenever the DB
            ;; is in HA role state, even if there is no live authority object
            ;; on this node. Followers can continue serving reads after replay
            ;; with only :ha-role/:store state, and falling back to a cached
            ;; :dt-db there leaks stale pre-replay views into remote queries.
            (db/refresh-cache store)
            (new-runtime-db store (current-runtime-opts m))))
        (:dt-db m))))))

(defn- remove-store
  [^Server server db-name]
  (let [m (get (.-dbs server) db-name)]
    (stop-ha-renew-loop m)
    (stop-ha-follower-sync-loop m)
    (stop-ha-authority db-name m)
    (when-let [store (:store m)]
      (if-let [db (:dt-db m)]
        (db/close-db db)
        (close-store store))))
  (.remove ^Map (.-dbs server) db-name))

(defn- update-cached-role
  [^Server server target-username]
  (sess/update-cached-role (session-deps) server target-username))

(defn- disconnect-client*
  [^Server server client-id]
  (sess/disconnect-client* (session-deps) server client-id))

(defn- disconnect-user
  [^Server server tgt-username]
  (sess/disconnect-user (session-deps) server tgt-username))

(defn- update-cached-permission
  [^Server server target-role]
  (sess/update-cached-permission (session-deps) server target-role))

;; networking

(defn- write-message
  "write a message to channel, auto grow the buffer"
  [^SelectionKey skey msg]
  (let [state                          (.attachment skey)
        {:keys [^ByteBuffer write-bf wire-opts]} @state
        ^SocketChannel  ch             (.channel skey)]
    (try
      (p/write-message-blocking ch write-bf msg wire-opts)
      (catch BufferOverflowException _
        (let [size (* ^long c/+buffer-grow-factor+ ^int (.capacity write-bf))]
          (vswap! state assoc :write-bf (bf/allocate-buffer size))
          (write-message skey msg))))))

(defn- handle-accept
  [^SelectionKey skey]
  (sdisp/handle-accept skey))

(defn- copy-in
  "Continuously read batched data from the client"
  [^Server server ^SelectionKey skey]
  (scopy/copy-in (copy-deps) server skey))

(defn- copy-out
  "Continiously write data out to client in batches"
  ([^SelectionKey skey data batch-size]
   (copy-out skey data batch-size nil nil))
  ([^SelectionKey skey data batch-size copy-meta]
   (copy-out skey data batch-size copy-meta nil))
  ([^SelectionKey skey data batch-size copy-meta response-meta]
   (scopy/copy-out (copy-deps) skey data batch-size copy-meta response-meta)))

(defn- copy-file-out
  "Stream a copied LMDB file to client as raw binary chunks with checksum."
  [^SelectionKey skey path copy-meta]
  (scopy/copy-file-out (copy-deps) skey path copy-meta))

(defn- cleanup-copy-tmp-dir*
  [tf]
  (scopy/cleanup-copy-tmp-dir* tf))

(def ^:private ^:redef cleanup-copy-tmp-dir-fn*
  scopy/cleanup-copy-tmp-dir-fn*)

(defn- cleanup-copy-tmp-dir!
  [tf]
  (scopy/cleanup-copy-tmp-dir! tf))

(def ^:private ^:redef server-copy-store!
  scopy/server-copy-store!)

(def ^:private ^:redef open-server-copied-store!
  scopy/open-server-copied-store!)

(def ^:private ^:redef close-server-copied-store!
  scopy/close-server-copied-store!)

(def ^:private ^:redef copy-server-file-out!
  copy-file-out)

(def ^:private ^:redef unpin-server-copy-backup-floor!
  scopy/unpin-server-copy-backup-floor!)

(defn- copy-source-kv-store
  [store]
  (scopy/copy-source-kv-store store))

(defn- copy-response-meta
  [db-name store base-meta]
  (scopy/copy-response-meta db-name store base-meta))

(defn- open-port
  [port]
  (try
    (doto (ServerSocketChannel/open)
      (.bind (InetSocketAddress. port))
      (.configureBlocking false))
    (catch Exception e
      (u/raise "Error opening port:" (ex-message e) {}))))

(defn- get-ip [^SelectionKey skey]
  (let [ch ^SocketChannel (.channel skey)]
    (.toString (.getAddress ^InetSocketAddress (.getRemoteAddress ch)))))

(defn- close-conn
  [^SelectionKey skey]
  (.close ^SocketChannel (.channel skey)))

(defn- client-disconnect?
  [e]
  (sdisp/client-disconnect? e))

(defn- handled-request-error?
  [e]
  (let [data     (ex-data e)
        err-data (:err-data data)]
    (and (instance? clojure.lang.ExceptionInfo e)
         (map? data)
         (nil? (ex-cause e))
         (or (:type data)
             (:error data)
             (:resized data)
             (map? err-data)))))

(defn- log-handled-request-error!
  [e]
  (let [data     (or (ex-data e) {})
        err-data (:err-data data)
        details  (cond-> {:message (ex-message e)}
                   (:type data) (assoc :type (:type data))
                   (:error data) (assoc :error (:error data))
                   (:db-name data) (assoc :db-name (:db-name data))
                   (map? err-data)
                   (cond->
                     (:type err-data) (assoc :err-type (:type err-data))
                     (:error err-data) (assoc :err-error (:error err-data))))]
    (if (handled-request-error? e)
      ;; These request failures are returned to the client and are often
      ;; asserted in tests. Keep them out of stderr unless debug logging is on.
      (log/debug "Handled request error" details)
      (log/error e))))

(defn- log-ha-loop-crash!
  [loop-name db-name t]
  (let [details (cond-> {:db-name db-name
                         :error-class (.getName ^Class (class t))}
                  (some? (ex-message t)) (assoc :message (ex-message t))
                  (some? (ex-data t)) (assoc :error-data (ex-data t)))]
    ;; HA loops retry after backoff. Keep operator-visible logs compact and
    ;; reserve the full stack trace for debug logging.
    (log/warn loop-name details)
    (log/debug t (str loop-name " stack trace") {:db-name db-name})))

(defn- close-conn-quietly
  [^SelectionKey skey]
  (try
    (close-conn skey)
    (catch Exception _ nil)))

(defn- error-response
  [^SelectionKey skey error-msg error-data]
  (let [{:keys [^ByteBuffer write-bf wire-opts]} @(.attachment skey)
        ^SocketChannel ch              (.channel skey)]
    (p/write-message-blocking ch write-bf
                              {:type     :error-response
                               :message  error-msg
                               :err-data error-data}
                              wire-opts)))

(defn- reopen-response
  [^SelectionKey skey msg]
  (let [{:keys [^ByteBuffer write-bf wire-opts]} @(.attachment skey)
        ^SocketChannel ch              (.channel skey)]
    (p/write-message-blocking ch write-bf msg wire-opts)))

(defn- handle-message-error!
  [^SelectionKey skey e]
  (sdisp/handle-message-error! (dispatch-deps) skey e))

(defmacro wrap-error
  [& body]
  `(try
     ~@body
     (catch Exception ~'e
       (handle-message-error! ~'skey ~'e))))

;; db

(defn- db-dir
  "translate from db-name to server db path"
  [root db-name]
  (str root u/+separator+ (u/hexify-string db-name)))

(defn- db-exists?
  [^Server server db-name]
  (u/file-exists
    (str (db-dir (.-root server) db-name) u/+separator+ c/data-file-name)))

(defn- dir->db-name
  [^Server server dir]
  (u/unhexify-string
    (s/replace-first dir (str (.-root server) u/+separator+) "")))

(defn- store->db-name
  [server store]
  (dir->db-name
    server
    (cond
      (instance? IStore store) (i/dir store)
      (instance? ILMDB store)  (i/env-dir store)
      :else                    (u/raise "Unknown store type" {}))))

(defn- detach-client-store!
  [^Server server ^SelectionKey skey db-name]
  (let [{:keys [client-id]} @(.attachment skey)]
    (update-client server client-id
                   #(-> %
                        (update :stores dissoc db-name)
                        (update :dt-dbs disj db-name)))))

(defn- db-store
  [^Server server ^SelectionKey skey db-name]
  (when (get (:stores (get-client server (:client-id @(.attachment skey))))
             db-name)
    (get-store server db-name)))

(defn- writing-lmdb
  [^Server server db-name]
  (get-in (.-dbs server) [db-name :wlmdb]))

(defn- writing-store
  [^Server server db-name]
  (get-in (.-dbs server) [db-name :wstore]))

(defn- store
  [^Server server ^SelectionKey skey db-name writing?]
  (or (if writing?
        (writing-store server db-name)
        (db-store server skey db-name))
      (u/raise "Store not found"
               {:type :reopen :db-name db-name :db-type "datalog"})))

(defn- lmdb
  [^Server server ^SelectionKey skey db-name writing?]
  (or (some-> (if writing?
                (writing-lmdb server db-name)
                (db-store server skey db-name))
              ((fn [store]
                 (if (instance? Store store)
                   (.-lmdb ^Store store)
                   store))))
      (u/raise "LMDB store not found"
               {:type :reopen :db-name db-name :db-type "kv"})))

(defn- store-closed?
  [store]
  (cond
    (nil? store)             true
    (instance? IStore store) (i/closed? store)
    (instance? ILMDB store)  (i/closed-kv? store)
    :else                    (u/raise "Unknown store type" {})))

(defn- store-in-use?
  [[db-name store]]
  (when-not (store-closed? store) db-name))

(defn- db-in-use?
  [server db-name]
  (when-let [store (get-store server db-name)]
    (not (store-closed? store))))

(defn- in-use-dbs [server] (keep store-in-use? (get-stores server)))

(defmacro normal-dt-store-handler
  "Handle request to Datalog store that needs no copy-in or copy-out"
  [f]
  `(write-message
     ~'skey
     {:type   :command-complete
      :result (apply
                ~(symbol "datalevin.interface" (str f))
                (store ~'server ~'skey (nth ~'args 0) ~'writing?)
                (rest ~'args))}))

(defmacro normal-kv-store-handler
  "Handle request to key-value store that needs no copy-in or copy-out"
  [f]
  `(write-message
     ~'skey
     {:type   :command-complete
      :result (apply
                ~(symbol "datalevin.interface" (str f))
                (lmdb ~'server ~'skey (nth ~'args 0) ~'writing?)
                (rest ~'args))}))

(defn- search-engine*
  [^Server server ^SelectionKey skey db-name]
  (when (get (:engines (get-client server
                                   (:client-id @(.attachment skey))))
             db-name)
    (get-in (.-dbs server) [db-name :engine])))

(defn- search-engine
  [^Server server ^SelectionKey skey db-name]
  (or (search-engine* server skey db-name)
      (u/raise "Search engine not found"
               {:type :reopen :db-name db-name :db-type "engine"})))

(defn- vector-index*
  [^Server server ^SelectionKey skey db-name]
  (when (get (:indices (get-client server (:client-id @(.attachment skey))))
             db-name)
    (get-in (.-dbs server) [db-name :index])))

(defn- vector-index
  [^Server server ^SelectionKey skey db-name]
  (or (vector-index* server skey db-name)
      (u/raise "Vector index not found"
               {:type :reopen :db-name db-name :db-type "index"})))

(defn- open-store
  [root db-name dbis datalog?]
  (let [dir (db-dir root db-name)]
    (if datalog?
      (do
        (dha/recover-ha-local-store-dir-if-needed! dir)
        (st/open dir))
      (let [lmdb (l/open-kv dir)]
        (doseq [dbi dbis] (i/open-dbi lmdb dbi))
        lmdb))))

(defn- reusable-open-store
  [store schema]
  (cond
    (instance? Store store)
    (when-not (i/closed? store)
      (let [lmdb (kv/raw-lmdb (.-lmdb ^Store store))]
        (if (and (nil? (i/get-value lmdb c/meta :last-modified :attr :long))
                 (empty? (i/get-range lmdb c/schema [:all] :attr :data)))
          ;; `conn/clear` can wipe DBIs via a raw KV handle while the server
          ;; still holds an open Datalog Store. Reopen from disk so runtime
          ;; schema/meta state matches the cleared LMDB contents.
          (let [env-dir (i/dir store)]
            (close-store store)
            (dha/recover-ha-local-store-dir-if-needed! env-dir)
            (st/open env-dir schema))
          (do
            ;; Preserve the legacy remote open-with-schema behavior for plain
            ;; stores, but never synthesize follower-local schema rows on HA
            ;; databases.
            (when (and schema
                       (nil? (:ha-mode (i/opts store))))
              (i/set-schema store schema))
            store))))

    (some? store)
    (when-not (i/closed-kv? store)
      store)

    :else
    nil))

(defn- effective-db-type
  [^Server server db-name requested-db-type]
  (or (some-> (pull-db (.-sys-conn server) db-name)
              :database/type)
      requested-db-type))

(defn- activate-runtime-on-open?
  [requested-db-type actual-db-type]
  (not (and (= requested-db-type c/kv-type)
            (= actual-db-type c/dl-type))))

(defn- reusable-store-for-db-type
  [store schema db-type]
  (case db-type
    :datalog
    (when (instance? Store store)
      (reusable-open-store store schema))

    (reusable-open-store store schema)))

(defn- multiple-lmdb-open-error?
  [e]
  (s/includes? (or (ex-message e) "")
               "Please do not open multiple LMDB connections"))

(defn- await-reusable-store
  [^Server server db-name schema db-type]
  (loop [attempts 40]
    (if-let [store (some-> (get-store server db-name)
                           (reusable-store-for-db-type schema db-type))]
      store
      (when (pos? attempts)
        (Thread/sleep (long 25))
        (recur (dec attempts))))))

(defn- db-open-lock
  [^Server server db-name]
  (let [dbs (.-dbs server)]
    (locking dbs
      (or (get-in dbs [db-name :open-lock])
          (let [lock (Object.)]
            (update-db server db-name #(assoc % :open-lock lock))
            lock)))))

(defn- db-write-admission-lock
  [^Server server db-name]
  (let [dbs (.-dbs server)]
    (locking dbs
      (or (get-in dbs [db-name :ha-write-admission-lock])
          (let [lock (Object.)]
            (update-db server db-name #(assoc % :ha-write-admission-lock lock))
            lock)))))

(defn- open-server-store
  "Open a store. NB. stores are left open"
  [^Server server ^SelectionKey skey
   {:keys [db-name schema opts return-db-info? respond?]
    :or   {respond? true}} requested-db-type]
  (wrap-error
    (let [{:keys [client-id]} @(.attachment skey)
          {:keys [username]}  (get-client server client-id)
          db-name             (u/lisp-case db-name)
          existing-db?        (db-exists? server db-name)
          sys-conn            (.-sys-conn server)]
      (log/debug "open" db-name "that exist?" existing-db?)
      (wrap-permission
          (if existing-db? ::view ::create)
          ::database
          (when existing-db? (db-eid sys-conn db-name))
          "Don't have permission to open database"
        (locking (db-open-lock server db-name)
          (let [dir              (db-dir (.-root server) db-name)
                existing-db-now? (db-exists? server db-name)
                db-type          (effective-db-type
                                   server db-name requested-db-type)
                activate-runtime? (activate-runtime-on-open?
                                    requested-db-type db-type)
                store            (or (some-> (get-store server db-name)
                                             (reusable-store-for-db-type
                                               schema db-type))
                                     (try
                                       (case db-type
                                         :datalog   (do
                                                      (dha/recover-ha-local-store-dir-if-needed! dir)
                                                      (st/open dir schema opts))
                                         :key-value (l/open-kv dir opts))
                                       (catch Exception e
                                         (if (multiple-lmdb-open-error? e)
                                           (or (await-reusable-store
                                                server db-name schema db-type)
                                               (throw e))
                                           (throw e)))))
                store            (try
                                   (add-store
                                     server db-name store activate-runtime? opts)
                                   (catch Throwable t
                                     (when (and (some? store)
                                                (not (store-closed? store)))
                                       (close-store store))
                                     (throw t)))
                 datalog?         (instance? Store store)
                 consensus-ha?    (and datalog?
                                       (some? (*consensus-ha-opts-fn* store)))]
            (update-client server client-id
                           #(cond-> %
                              true     (update :stores assoc db-name
                                               {:datalog? datalog?
                                                :dbis     #{}
                                                :consensus-ha? consensus-ha?})
                              (and datalog? activate-runtime?)
                              (update :dt-dbs conj db-name)))
            (when-not existing-db-now?
              (transact-new-db sys-conn username db-type db-name)
              (update-client server client-id
                             #(assoc % :permissions
                                     (user-permissions sys-conn username))))
            (let [db-info (when (and return-db-info? datalog?)
                            (assoc (fresh-runtime-db-info store)
                                   :opts (i/opts store)))]
              (when respond?
                (write-message skey
                               (cond-> {:type :command-complete}
                                 db-info (assoc :result db-info))))
              db-info)))))))

(defn- session-lmdb [sys-conn] (sess/session-lmdb sys-conn))

(defn get-default-password
  "Return the initial admin password, checking DATALEVIN_DEFAULT_PASSWORD
  environment variable first, falling back to the built-in default."
  []
  (or (System/getenv "DATALEVIN_DEFAULT_PASSWORD")
      c/default-password))

(defn- init-sys-db
  [root password]
  (let [sys-conn (d/get-conn (str root u/+separator+ c/system-dir)
                             server-schema)]
    (when (= 0 (i/datom-count (.-store ^DB (d/db sys-conn)) c/eav))
      (let [s (salt)
            h (password-hashing password s)
            txs [{:db/id        -1
                  :user/name    c/default-username
                  :user/pw-hash h
                  :user/pw-salt s}
                 {:db/id    -2
                  :role/key (user-role-key c/default-username)}
                 {:db/id          -3
                  :user-role/user -1
                  :user-role/role -2}
                 {:db/id          -4
                  :permission/act ::control
                  :permission/obj ::server}
                 {:db/id          -5
                  :role-perm/perm -4
                  :role-perm/role -2}]]
        (d/transact! sys-conn txs)))
    sys-conn))

(defn- load-sessions
  [sys-conn]
  (sess/load-sessions sys-conn))

(defn- reopen-dbs
  [root clients ^ConcurrentHashMap dbs]
  (sess/reopen-dbs (session-deps) root clients dbs))

(defn- authenticate
  [^Server server ^SelectionKey skey {:keys [username password]}]
  (sess/authenticate (session-deps) server skey
                     {:username username :password password}))

(defn- client-display
  [^Server server [client-id m]]
  (sess/client-display (session-deps) server [client-id m]))


;; Server-owned option-mutation helpers used by extracted message handlers.

(declare cleanup-assoc-opt-rollback-backup!)

(defn- prepare-assoc-opt-rollback-backup!
  [^Server server db-name store k]
  (when (and (instance? Store store)
             (or (contains? ha-runtime-option-key-set k)
                 (some? (resolved-ha-runtime-opts (.-root server)
                                                 db-name
                                                 store))))
    (let [current-state (get (.-dbs server) db-name)
          runtime-ha-opts (resolved-ha-runtime-opts
                           (.-root server)
                           db-name
                           store
                           current-state)]
      {:ha-runtime-opts runtime-ha-opts})))

(defn- reject-unsafe-live-ha-option-mutation!
  [^Server server db-name store k old-opts new-opts]
  (when (and (= k :ha-members)
             (not= old-opts new-opts)
             (instance? Store store)
             (some? (resolved-ha-runtime-opts
                     (.-root server)
                     db-name
                     store
                     (get (.-dbs server) db-name))))
    (u/raise "Option :ha-members cannot be changed via assoc-opt on a live consensus HA database"
             {:error :ha/unsafe-live-option-mutation
              :db-name db-name
              :option k})))

(defn- cleanup-assoc-opt-rollback-backup!
  [{:keys [backup-root]}]
  (when (and (string? backup-root)
             (u/file-exists backup-root))
    (u/delete-files backup-root)))

(defn- restore-assoc-opt-rollback-backup!
  [env-dir {:keys [backup-dir]}]
  (when (and (string? backup-dir)
             (u/file-exists backup-dir))
    (when (u/file-exists env-dir)
      (u/delete-files env-dir))
    (#'dha/copy-dir-contents! backup-dir env-dir)
    true))

(defn- rollback-assoc-opt!
  [^Server server db-name store old-opts k rollback-backup]
  (when (and (instance? Store store)
             old-opts
             (not= old-opts (i/opts store)))
    (try
      (let [env-dir (i/dir store)
            schema (i/schema store)]
        (#'st/transact-opts (.-lmdb ^Store store) old-opts)
        (when-not (store-closed? store)
          (close-store store))
        (dha/recover-ha-local-store-dir-if-needed! env-dir)
        (add-store server db-name
                   (st/open env-dir schema old-opts)
                   true
                   (:ha-runtime-opts rollback-backup)))
      (catch Throwable rollback-t
        (log/error rollback-t
                   "Failed to roll back store option mutation"
                   {:db-name db-name
                    :option k}))
      (finally
        (cleanup-assoc-opt-rollback-backup! rollback-backup)))))

(defn- apply-assoc-opt!
  [^Server server db-name store writing? k v]
  (let [old-opts (when (instance? IStore store)
                   (i/opts store))
        k' (c/canonical-wal-option-key k)
        new-opts (when old-opts
                   (-> old-opts
                       (dissoc k)
                       (assoc k' v)))]
    (if (and old-opts (= old-opts new-opts))
      old-opts
      (let [rollback-backup (when-not writing?
                              (prepare-assoc-opt-rollback-backup!
                               server db-name store k'))]
        (try
          (when-not writing?
            (reject-unsafe-live-ha-option-mutation!
             server db-name store k' old-opts new-opts))
          (let [result (i/assoc-opt store k' v)]
            ;; For direct mutations, make runtime restart part of the same
            ;; logical operation as the store option change.
            (when-not writing?
              (add-store server db-name store true))
            result)
          (catch Throwable t
            (when-not writing?
              (rollback-assoc-opt! server
                                   db-name
                                   store
                                   old-opts
                                   k'
                                   rollback-backup))
            (throw t))
          (finally
            (when-not writing?
              (cleanup-assoc-opt-rollback-backup! rollback-backup))))))))

(defn- get-lock
  [^Server server db-name]
  (let [dbs (.-dbs server)]
    (locking dbs
      (or (get-in dbs [db-name :lock])
          (let [lock (Semaphore. 1)]
            (update-db server db-name #(assoc % :lock lock))
            lock)))))

(defn- get-runtime-access-lock
  [^Server server db-name]
  (let [dbs (.-dbs server)]
    (locking dbs
      (or (get-in dbs [db-name :runtime-access-lock])
          (let [lock (ReentrantReadWriteLock. true)]
            (update-db server db-name #(assoc % :runtime-access-lock lock))
            lock)))))

(defn with-db-runtime-store-read-access
  "Run `f` while holding the runtime-store read lock for `db-name`.

  This coordinates readers against runtime store swaps and shutdown in callers
  that access the live store directly."
  [^Server server db-name f]
  (let [dbs (.-dbs server)]
    (if (and db-name (.containsKey ^ConcurrentHashMap dbs db-name))
      (let [^ReentrantReadWriteLock lock (get-runtime-access-lock server db-name)
            read-lock                    (.readLock lock)]
        (.lock read-lock)
        (try
          (f)
          (finally
            (.unlock read-lock))))
      (f))))

(defn- with-db-runtime-read-access
  [^Server server message f]
  (with-db-runtime-store-read-access server (nth (:args message) 0 nil) f))

(defn- with-db-runtime-store-swap
  [^Server server db-name f]
  (if db-name
    (let [^ReentrantReadWriteLock lock (get-runtime-access-lock server db-name)
          write-lock                   (.writeLock lock)]
      (.lock write-lock)
      (try
        (f)
        (finally
          (.unlock write-lock))))
    (f)))

(defn- get-kv-store
  [server db-name]
  (let [s (get-store server db-name)]
    (or (when s
          (if (instance? Store s) (.-lmdb ^Store s) s))
        (u/raise "LMDB store not found"
                 {:type :reopen :db-name db-name :db-type "kv"}))))

(declare dispatch-message)
 (declare trace-remote-tx!)

(defn- current-ha-txlog-term
  [^Server server db-name]
  (sdisp/current-ha-txlog-term (dispatch-deps) server db-name))

(defn- dispatch-message-with-ha-write-admission
  [^Server server ^SelectionKey skey message]
  (sdisp/dispatch-message-with-ha-write-admission
   (dispatch-deps) server skey message))

(defprotocol IRunner
  "Ensure calls within `with-transaction-kv` run in the same thread that
  runs `open-transact-kv`, otherwise LMDB will deadlock"
  (new-message [this skey message])
  (run-calls [this])
  (halt-run [this]))

(def ^:private runner-stop-signal ::runner-stop)

(deftype Runner [server ^LinkedBlockingQueue queue running?]
  IRunner
  (new-message [_ skey message]
    (trace-remote-tx! "runner-enqueue" (:type message) (nth (:args message) 0 nil))
    (.put queue [skey message]))

  (halt-run [_]
    (vreset! running? false)
    (.clear queue)
    (.offer queue runner-stop-signal))

  (run-calls [_]
    (loop []
      (let [item (.take queue)]
        (when-not (= runner-stop-signal item)
          (let [[skey message] item]
            (trace-remote-tx! "runner-dispatch" (:type message)
                              (nth (:args message) 0 nil))
            (dispatch-message-with-ha-write-admission server skey message))
          (when @running?
            (recur)))))))

(defn- write-txn-runner
  [^Server server db-name kv-store]
  (let [runner (->Runner server (LinkedBlockingQueue.) (volatile! true))]
    (update-db server db-name #(assoc % :runner runner))
    runner))

(defn- handler-deps
  []
  {:add-store add-store
   :apply-assoc-opt! apply-assoc-opt!
   :authenticate authenticate
   :cleanup-copy-tmp-dir! cleanup-copy-tmp-dir!
   :client-display client-display
   :close-server-copied-store! close-server-copied-store!
   :copy-in copy-in
   :copy-out copy-out
   :copy-response-meta copy-response-meta
   :copy-server-file-out! copy-server-file-out!
   :current-runtime-opts current-runtime-opts
   :db-dir db-dir
   :db-exists? db-exists?
   :db-in-use? db-in-use?
   :db-state (fn [^Server server db-name]
               (get (.-dbs server) db-name))
   :db-store db-store
   :dbs (fn [^Server server] (.-dbs server))
   :detach-client-store! detach-client-store!
   :disconnect-client* disconnect-client*
   :disconnect-user disconnect-user
   :get-client get-client
   :get-db get-db
   :get-kv-store get-kv-store
   :get-lock get-lock
   :get-store get-store
   :halt-run halt-run
   :handle-message-error! handle-message-error!
   :in-use-dbs in-use-dbs
   :lmdb lmdb
   :new-runtime-db new-runtime-db
   :open-server-copied-store! open-server-copied-store!
   :open-server-store open-server-store
   :open-write-txn-with-retry open-write-txn-with-retry
   :remove-client remove-client
   :remove-store remove-store
   :root (fn [^Server server] (.-root server))
   :run-calls run-calls
   :search-engine search-engine
   :search-engine* search-engine*
   :server-copy-store! server-copy-store!
   :store store
   :store->db-name store->db-name
   :store-closed? store-closed?
   :sys-conn (fn [^Server server] (.-sys-conn server))
   :unpin-server-copy-backup-floor! unpin-server-copy-backup-floor!
   :update-cached-permission update-cached-permission
   :update-cached-role update-cached-role
   :update-client update-client
   :update-db update-db
   :vector-index vector-index
   :with-db-runtime-store-read-access with-db-runtime-store-read-access
   :write-message write-message
   :write-txn-runner write-txn-runner
   :clients (fn [^Server server] (.-clients server))})

(def ^:private message-handler-map
  (into {}
        (map (fn [[type handler]]
               [type
                (fn [server skey message]
                  (handler (handler-deps) server skey message))]))
        sh/handler-map))

(defn- dispatch-message
  [^Server server ^SelectionKey skey message]
  (sdisp/dispatch-message (dispatch-deps) server skey message))

(defn- execute
  "Execute a function in a thread from the worker thread pool"
  [^Server server f]
  (sdisp/execute (dispatch-deps) server f))

(def ^:private trace-remote-tx?
  (some? (System/getenv "DTLV_TRACE_REMOTE_TX")))

(defn- trace-remote-tx!
  [& xs]
  (when trace-remote-tx?
    (binding [*out* *err*]
      (apply println xs)
      (flush))))

(def ^:private idempotent-withtxn-control-types
  #{:close-transact
    :abort-transact
    :close-transact-kv
    :abort-transact-kv})

(defn- handle-writing
  [^Server server ^SelectionKey skey {:keys [args] :as message}]
  (sdisp/handle-writing (dispatch-deps) server skey message))

(defn- set-last-active
  [^Server server ^SelectionKey skey]
  nil)

(defn- handle-message
  [^Server server ^SelectionKey skey fmt msg ]
  (sdisp/handle-message (dispatch-deps) server skey fmt msg))

(defn- handle-read
  [^Server server ^SelectionKey skey]
  (sdisp/handle-read (dispatch-deps) server skey))

(defn- handle-registration
  [^Server server]
  (let [^Selector selector           (.-selector server)
        ^ConcurrentLinkedQueue queue (.-register-queue server)]
    (loop []
      (when-let [[^SocketChannel ch ops state] (.poll queue)]
        (.register ch selector ops state)
        (log/debug "Registered client" (@state :client-id))
        (recur)))))

(defn- remove-idle-sessions
  [^Server server]
  (sess/remove-idle-sessions (session-deps) server))

(defn- event-loop
  [^Server server]
  (let [^Selector selector     (.-selector server)
        ^AtomicBoolean running (.-running server)]
    (loop []
      (when (.get running)
        (remove-idle-sessions server)
        (handle-registration server)
        (.select selector)
        (when (.get running)
          (let [^Iterator iter (-> selector (.selectedKeys) (.iterator))]
            (loop []
              (when (.hasNext iter)
                (let [^SelectionKey skey (.next iter)]
                  (when (and (.isValid skey) (.isAcceptable skey))
                    (handle-accept skey))
                  (when (and (.isValid skey) (.isReadable skey))
                    (handle-read server skey)))
                (.remove iter)
                (recur)))))
        (recur)))))

(defn- session-deps
  []
  {:sys-conn-fn (fn [^Server server] (.-sys-conn server))
   :clients-fn (fn [^Server server] (.-clients server))
   :selector-fn (fn [^Server server] (.-selector server))
   :user-roles-fn user-roles
   :user-permissions-fn user-permissions
   :user-eid-fn user-eid
   :perm-tgt-name-fn perm-tgt-name
   :open-store-fn open-store
   :close-store-fn close-store
   :consensus-ha-opts-fn *consensus-ha-opts-fn*
   :resolved-runtime-opts-fn resolved-runtime-opts
   :ensure-ha-runtime-fn (fn [root db-name m store]
                           (ensure-ha-runtime root db-name m store))
   :new-runtime-db-fn new-runtime-db
   :current-runtime-opts-fn current-runtime-opts
   :pull-user-fn pull-user
   :password-matches?-fn password-matches?
   :get-ip-fn get-ip
   :close-conn-fn close-conn
   :idle-timeout-fn (fn [^Server server] (.-idle-timeout server))})

(defn- copy-deps
  []
  {:register-queue-fn (fn [^Server server] (.-register-queue server))
   :write-message-fn write-message})

(defn- dispatch-deps
  []
  {:close-conn-fn close-conn
   :dbs-fn (fn [^Server server] (.-dbs server))
   :with-ha-write-admission-fn with-ha-write-admission
   :ha-write-commit-check-fn-fn ha-write-commit-check-fn
   :cleanup-rejected-close-transact!-fn cleanup-rejected-close-transact!
   :message-handler-map message-handler-map
   :work-executor-fn (fn [^Server server] (.-work-executor server))
   :trace-remote-tx-fn trace-remote-tx!
   :get-kv-store-fn get-kv-store
   :new-message-fn new-message
   :write-message-fn write-message
   :get-client-fn get-client
   :clients-fn (fn [^Server server] (.-clients server))
   :with-db-runtime-read-access-fn with-db-runtime-read-access})

(defn- ha-deps
  []
  {:get-lock-fn get-lock
   :db-write-admission-lock-fn db-write-admission-lock
   :dbs-fn (fn [^Server server] (.-dbs server))
   :replace-db-state-if-current-fn replace-db-state-if-current
   :transform-db-state-when-fn transform-db-state-when
   :with-db-runtime-store-read-access-fn with-db-runtime-store-read-access
   :with-db-runtime-store-swap-fn with-db-runtime-store-swap
   :ha-renew-step-fn ha-renew-step
   :ha-follower-sync-step-fn ha-follower-sync-step
   :persist-ha-follower-side-effects!-fn persist-ha-follower-side-effects!
   :running-fn (fn [^Server server] (.-running server))
   :work-executor-fn (fn [^Server server] (.-work-executor server))
   :update-db-fn update-db
   :current-runtime-opts-fn current-runtime-opts
   :stop-ha-renew-loop-fn *stop-ha-renew-loop-fn*
   :stop-ha-follower-sync-loop-fn *stop-ha-follower-sync-loop-fn*
   :await-ha-loop-stop-fn await-ha-loop-stop
   :stop-ha-authority-fn *stop-ha-authority-fn*
   :start-ha-authority-fn *start-ha-authority-fn*
   :consensus-ha-opts-fn *consensus-ha-opts-fn*
   :ensure-udf-readiness-state-fn *ensure-udf-readiness-state-fn*
   :udf-admission-exempt-write-types udf-admission-exempt-write-types
   :udf-write-admission-error-fn udf-write-admission-error
   :get-kv-store-fn get-kv-store
   :halt-run-fn halt-run
   :log-ha-loop-crash!-fn log-ha-loop-crash!
   :sleep-ha-loop-fn sleep-ha-loop!
   :ha-loop-sleep-ms-fn ha-loop-sleep-ms
   :ha-follower-loop-sleep-ms-fn ha-follower-loop-sleep-ms
   :ha-loop-error-backoff-fn ha-loop-error-backoff!})

(defn create
  "Create a Datalevin server. Initially not running, call `start` to run."
  [{:keys [port root idle-timeout verbose]
    :as   opts
    :or   {port         8898
           root         "/var/lib/datalevin"
           idle-timeout c/default-idle-timeout
           verbose      false}}]
  {:pre [(int? port) (not (s/blank? root))]}
  (try
    (when (contains? opts :verbose)
      (log/set-min-level! (if verbose :debug :info)))
    (let [^ServerSocketChannel server-socket (open-port port)
          ^Selector selector                 (Selector/open)
          running                            (AtomicBoolean. false)
          sys-conn                           (init-sys-db root (get-default-password))
          clients                            (load-sessions sys-conn)
          dbs                                (ConcurrentHashMap.)]
      (reopen-dbs root clients dbs)
      (.register server-socket selector SelectionKey/OP_ACCEPT)
      (let [server (->Server running
                             port
                             root
                             idle-timeout
                             server-socket
                             selector
                             (ConcurrentLinkedQueue.)
                             (Executors/newSingleThreadExecutor)
                             (Executors/newCachedThreadPool) ; with-txn may be many
                             sys-conn
                             clients
                             dbs)]
        (doseq [db-name (keys dbs)]
          (ensure-ha-renew-loop server db-name)
          (ensure-ha-follower-sync-loop server db-name))
        server))
    (catch Exception e
      (u/raise "Error creating server:" (ex-message e) {}))))
