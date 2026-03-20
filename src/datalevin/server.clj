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
   [datalevin.dump :as dump]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.query :as q]
   [datalevin.db :as db]
   [datalevin.udf :as udf]
   [datalevin.lmdb :as l]
   [datalevin.binding.cpp :as cpp]
   [datalevin.protocol :as p]
   [datalevin.storage :as st]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ctrl]
   [datalevin.server.auth :as auth]
   [datalevin.server.ha-runtime :as hrt]
   [datalevin.search :as sc]
   [datalevin.txlog :as txlog]
   [datalevin.vector :as v]
   [datalevin.kv :as kv]
   [datalevin.built-ins :as dbq]
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
    ConcurrentLinkedQueue ConcurrentHashMap CountDownLatch Semaphore TimeUnit]
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
      (dha/recover-ha-local-store-dir-if-needed! env-dir)
      (st/open env-dir (i/schema store) (i/opts store)))

    (instance? ILMDB store)
    (l/open-kv (i/env-dir store) (i/env-opts store))

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
          result (locking kv-store
                   (try
                     {:ok? true
                      :store store
                      :kv-store kv-store
                      :wlmdb (i/open-transact-kv kv-store)}
                     (catch Throwable t
                       {:ok? false
                        :store store
                        :error t})))]
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

(declare event-loop close-conn store->db-name session-lmdb remove-store)

(def session-dbi "datalevin-server/sessions")

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
        (.submit dispatcher ^Callable init)
        (.set running true))))

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
  (get (.-clients server) client-id))

(defn- add-client
  [^Server server ip client-id username]
  (let [sys-conn (.-sys-conn server)
        roles    (user-roles sys-conn username)
        perms    (user-permissions sys-conn username)
        session  {:ip          ip
                  :uid         (user-eid sys-conn username)
                  :username    username
                  :last-active (System/currentTimeMillis)
                  :stores      {}
                  :engines     #{}
                  :indices     #{}
                  :dt-dbs      #{}
                  :roles       roles
                  :permissions perms}]
    (d/transact-kv (session-lmdb sys-conn)
                   [(l/kv-tx :put session-dbi client-id session :uuid :data)])
    (.put ^Map (.-clients server) client-id session)
    (log/info "Added client " client-id
              "from:" ip
              "for user:" username)))

(defn- remove-client
  [^Server server client-id]
  (d/transact-kv (session-lmdb (.-sys-conn server))
                 [(l/kv-tx :del session-dbi client-id :uuid)])
  (.remove ^Map (.-clients server) client-id)
  (log/info "Removed client:" client-id))

(defn- update-client
  [^Server server client-id f]
  (let [session (f (get-client server client-id))]
    (d/transact-kv (session-lmdb (.-sys-conn server))
                   [(l/kv-tx :put session-dbi client-id session :uuid :data)])
    (.put ^Map (.-clients server) client-id session)))

(declare get-store store-closed?)

(defn- get-stores
  [^Server server]
  (into {}
        (keep (fn [[db-name _]]
                (when-let [store (get-store server db-name)]
                  [db-name store])))
        (.-dbs server)))

(defn- get-store
  ([^Server server db-name writing?]
   (let [m (get (.-dbs server) db-name)
         usable-store
         (fn [store]
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
         runtime-store
         (fn [dt-db]
           (when (instance? DB dt-db)
             (usable-store (.-store ^DB dt-db))))]
     (if writing?
       (or (usable-store (:wstore m))
           (runtime-store (:wdt-db m)))
       (or (usable-store (:store m))
           (runtime-store (:dt-db m))))))
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

(def ^:private missing-state-value hrt/missing-state-value)
(def ^:private ha-follower-local-side-effect-keys
  hrt/ha-follower-local-side-effect-keys)
(def ^:private ha-follower-side-effect-keys
  hrt/ha-follower-side-effect-keys)
(def ^:private state-patch hrt/state-patch)
(def ^:private ha-follower-local-side-effect-patch
  hrt/ha-follower-local-side-effect-patch)
(def ^:private ha-follower-side-effect-patch
  hrt/ha-follower-side-effect-patch)
(def ^:private ha-renew-merge-excluded-keys
  hrt/ha-renew-merge-excluded-keys)
(def ^:private ha-renew-state-patch hrt/ha-renew-state-patch)
(def ^:private apply-state-patch hrt/apply-state-patch)
(def ^:private same-ha-runtime-state? hrt/same-ha-runtime-state?)
(def ^:private merge-ha-follower-local-side-effect-patch
  hrt/merge-ha-follower-local-side-effect-patch)
(def ^:private merge-ha-follower-side-effect-patch
  hrt/merge-ha-follower-side-effect-patch)
(def ^:private merge-ha-renew-state-patch
  hrt/merge-ha-renew-state-patch)
(def ^:private persist-ha-follower-side-effects!
  hrt/persist-ha-follower-side-effects!)

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

(defn- new-runtime-db
  [store runtime-opts]
  (attach-runtime-opts (db/new-db store) runtime-opts))

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

(def consensus-ha-opts hrt/consensus-ha-opts)

(def ^:dynamic *consensus-ha-opts-fn*
  consensus-ha-opts)

(def ^:private ha-runtime-option-keys hrt/ha-runtime-option-keys)
(def ^:private ha-runtime-option-key-set hrt/ha-runtime-option-key-set)
(def ^:private sanitize-ha-path-segment hrt/sanitize-ha-path-segment)
(def ^:private default-ha-control-raft-dir hrt/default-ha-control-raft-dir)
(def ^:private with-default-ha-control-raft-dir
  hrt/with-default-ha-control-raft-dir)
(def ^:private start-ha-authority hrt/start-ha-authority)
(def ^:private stop-ha-authority hrt/stop-ha-authority)

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

(declare get-lock db-write-admission-lock with-db-runtime-store-swap)

(defn- ha-follower-apply-record-with-guard
  [^Server server db-name expected-state record]
  (let [^Semaphore lock (get-lock server db-name)]
    (.acquire lock)
    (try
      (locking (db-write-admission-lock server db-name)
        (let [current-state (get (.-dbs server) db-name)]
          (if (and current-state
                   (= :follower (:ha-role current-state))
                   (same-ha-runtime-state?
                    current-state
                    expected-state
                    :ha-follower-loop-running?))
            (dha/apply-ha-follower-txlog-record! expected-state record)
            (u/raise "HA follower replay aborted because follower state changed"
                     {:error :ha/follower-stale-state
                      :db-name db-name
                      :record-lsn (:lsn record)
                      :state current-state
                      :current-role (:ha-role current-state)
                      :expected-role (:ha-role expected-state)}))))
      (finally
        (.release lock)))))

(def ^:private ha-loop-sleep-ms hrt/ha-loop-sleep-ms)
(def ^:private ha-follower-loop-sleep-ms hrt/ha-follower-loop-sleep-ms)
(def ^:private sleep-ha-loop! hrt/sleep-ha-loop!)
(def ^:private ha-loop-error-backoff! hrt/ha-loop-error-backoff!)

(defn- run-ha-renew-loop
  [^Server server db-name ^AtomicBoolean running? ^CountDownLatch stopped-latch]
  (try
    (loop []
      (when (and (.get running?)
                 (.get ^AtomicBoolean (.-running server)))
        (try
          (let [m (get (.-dbs server) db-name)]
            (if (or (nil? m)
                    (nil? (:ha-authority m))
                    (not (identical? running?
                                     (:ha-renew-loop-running? m))))
              (.set running? false)
              ;; Keep renew work outside `update-db` so HA probes and peer/server
              ;; operations do not block on control-plane I/O.
              (let [next-state (ha-renew-step db-name m)
                    renew-patch (ha-renew-state-patch m next-state)
                    {:keys [updated? state]}
                    (replace-db-state-if-current
                     server
                     db-name
                     m
                     #(identical? running? (:ha-renew-loop-running? %))
                     next-state)
                    state
                    (if (and (not updated?) renew-patch)
                      (:state
                       (transform-db-state-when
                        server
                        db-name
                        #(identical? running? (:ha-renew-loop-running? %))
                        #(merge-ha-renew-state-patch
                          %
                          m
                          renew-patch)))
                      state)]
                (if (or (nil? state)
                        (nil? (:ha-authority state))
                        (not (identical? running?
                                         (:ha-renew-loop-running? state))))
                  (.set running? false)
                  (sleep-ha-loop! running? (ha-loop-sleep-ms state))))))
          (catch Throwable t
            (log/error t "HA renew loop crashed"
                       {:db-name db-name})
            (ha-loop-error-backoff! running?)))
        (recur)))
    (finally
      (.countDown stopped-latch))))

(defn- run-ha-follower-sync-loop
  [^Server server db-name ^AtomicBoolean running? ^CountDownLatch stopped-latch]
  (try
    (loop []
      (when (and (.get running?)
                 (.get ^AtomicBoolean (.-running server)))
        (try
          (let [m (get (.-dbs server) db-name)]
            (if (or (nil? m)
                    (nil? (:ha-authority m))
                    (not (identical? running?
                                     (:ha-follower-loop-running? m))))
              (.set running? false)
              ;; Follower replay can block on remote txlog fetch and local apply
              ;; work. Keep it off the authority renew path so lease reads and
              ;; promotions are not rate-limited by replication latency.
              (let [next-state (binding [dha/*ha-follower-apply-record-fn*
                                         (fn [state record]
                                           (ha-follower-apply-record-with-guard
                                            server
                                            db-name
                                            state
                                            record))
                                         dha/*ha-with-local-store-swap-fn*
                                         (fn [f]
                                           (with-db-runtime-store-swap
                                             server
                                             db-name
                                             f))]
                                 (ha-follower-sync-step db-name m))
                    local-patch (ha-follower-local-side-effect-patch
                                 m next-state)
                    side-effect-patch (ha-follower-side-effect-patch
                                       m next-state)
                    _ (persist-ha-follower-side-effects!
                       m next-state local-patch)
                    {:keys [updated? state]}
                    (replace-db-state-if-current
                     server
                     db-name
                     m
                     #(identical? running? (:ha-follower-loop-running? %))
                     next-state)
                    state
                    (if (and (not updated?)
                             (or local-patch side-effect-patch))
                      (:state
                       (transform-db-state-when
                        server
                        db-name
                        #(identical? running? (:ha-follower-loop-running? %))
                        #(merge-ha-follower-side-effect-patch
                          %
                          m
                          local-patch
                          side-effect-patch)))
                      state)]
                (if (or (nil? state)
                        (nil? (:ha-authority state))
                        (not (identical? running?
                                         (:ha-follower-loop-running? state))))
                  (.set running? false)
                  (sleep-ha-loop! running?
                                  (ha-follower-loop-sleep-ms state))))))
          (catch Throwable t
            (log/error t "HA follower sync loop crashed"
                       {:db-name db-name})
            (ha-loop-error-backoff! running?)))
        (recur)))
    (finally
      (.countDown stopped-latch))))

(declare execute)

(defn- ensure-ha-renew-loop
  [^Server server db-name]
  (let [new-running-v (volatile! nil)]
    (update-db server db-name
      (fn [m]
        (if (and m
                 (:ha-authority m))
          (let [running?    (:ha-renew-loop-running? m)
                loop-future (:ha-renew-loop-future m)
                active?     (and (instance? AtomicBoolean running?)
                                 (.get ^AtomicBoolean running?)
                                 (instance? Future loop-future)
                                 (not (.isDone ^Future loop-future)))]
            (if active?
              m
              (do
                (when (instance? AtomicBoolean running?)
                  (.set ^AtomicBoolean running? false))
                (let [new-running?  (AtomicBoolean. true)
                      stopped-latch (CountDownLatch. 1)]
                  (vreset! new-running-v new-running?)
                  (assoc m
                         :ha-renew-loop-running? new-running?
                         :ha-renew-loop-stopped-latch stopped-latch
                         :ha-renew-loop-future nil)))))
          m)))
    (when-let [running? @new-running-v]
      (let [stopped-latch
            (get-in (.-dbs server) [db-name :ha-renew-loop-stopped-latch])
            future (.submit ^ExecutorService
                            (.-work-executor server)
                            ^Runnable #(run-ha-renew-loop
                                         server
                                         db-name
                                         running?
                                         stopped-latch))]
        (update-db server db-name
          (fn [m]
            (if (and m
                     (identical? running?
                                 (:ha-renew-loop-running? m)))
              (assoc m :ha-renew-loop-future future)
              m)))))))

(defn- ensure-ha-follower-sync-loop
  [^Server server db-name]
  (let [new-running-v (volatile! nil)]
    (update-db server db-name
      (fn [m]
        (if (and m
                 (:ha-authority m))
          (let [running?    (:ha-follower-loop-running? m)
                loop-future (:ha-follower-loop-future m)
                active?     (and (instance? AtomicBoolean running?)
                                 (.get ^AtomicBoolean running?)
                                 (instance? Future loop-future)
                                 (not (.isDone ^Future loop-future)))]
            (if active?
              m
              (do
                (when (instance? AtomicBoolean running?)
                  (.set ^AtomicBoolean running? false))
                (let [new-running?  (AtomicBoolean. true)
                      stopped-latch (CountDownLatch. 1)]
                  (vreset! new-running-v new-running?)
                  (assoc m
                         :ha-follower-loop-running? new-running?
                         :ha-follower-loop-stopped-latch stopped-latch
                         :ha-follower-loop-future nil)))))
          m)))
    (when-let [running? @new-running-v]
      (let [stopped-latch
            (get-in (.-dbs server) [db-name :ha-follower-loop-stopped-latch])
            future (.submit ^ExecutorService
                            (.-work-executor server)
                            ^Runnable #(run-ha-follower-sync-loop
                                         server
                                         db-name
                                         running?
                                         stopped-latch))]
        (update-db server db-name
          (fn [m]
            (if (and m
                     (identical? running?
                                 (:ha-follower-loop-running? m)))
              (assoc m :ha-follower-loop-future future)
              m)))))))

(defn- stop-ha-renew-loop
  [m]
  (when-let [^AtomicBoolean running? (:ha-renew-loop-running? m)]
    (.set running? false))
  (when-let [^Future future (:ha-renew-loop-future m)]
    (.cancel future true)))

(defn- stop-ha-follower-sync-loop
  [m]
  (when-let [^AtomicBoolean running? (:ha-follower-loop-running? m)]
    (.set running? false))
  (when-let [^Future future (:ha-follower-loop-future m)]
    (.cancel future true)))

(def ^:private await-ha-loop-stop hrt/await-ha-loop-stop)

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
  (hrt/current-ha-runtime-local-opts m current-runtime-opts))

(defn- resolved-ha-runtime-opts
  ([root db-name store]
   (resolved-ha-runtime-opts root db-name store nil nil))
  ([root db-name store m]
   (resolved-ha-runtime-opts root db-name store m nil))
  ([root db-name store m explicit-ha-runtime-opts]
   (hrt/resolved-ha-runtime-opts
    root db-name store m explicit-ha-runtime-opts
    {:consensus-ha-opts-fn *consensus-ha-opts-fn*
     :current-runtime-opts-fn current-runtime-opts})))

(def ^:private shared-store-lifecycle? hrt/shared-store-lifecycle?)

(defn- stop-ha-runtime
  [db-name m]
  (hrt/stop-ha-runtime
   db-name
   m
   {:current-runtime-opts-fn current-runtime-opts
    :stop-ha-renew-loop-fn *stop-ha-renew-loop-fn*
    :stop-ha-follower-sync-loop-fn *stop-ha-follower-sync-loop-fn*
    :await-ha-loop-stop-fn await-ha-loop-stop
    :stop-ha-authority-fn *stop-ha-authority-fn*}))

(def ^:private ha-authority-running? hrt/ha-authority-running?)

(declare db-write-admission-lock)

(defn- ha-write-admission-error
  [^Server server message]
  (let [write?  (dha/ha-write-message? message)
        db-name (nth (:args message) 0 nil)
        m0      (when (and db-name (contains? (.-dbs server) db-name))
                  (if write?
                    (update-db server db-name *ensure-udf-readiness-state-fn*)
                    (get (.-dbs server) db-name)))
        m       m0]
    (or (and db-name (udf-write-admission-error db-name m))
        (dha/ha-write-admission-error (.-dbs server) message))))

(defn- ha-write-commit-admission!
  [^Server server message]
  (let [db-name (nth (:args message) 0 nil)]
    (when db-name
      (update-db
        server
        db-name
        (fn [m]
          (if (and (= :leader (:ha-role m))
                   (satisfies? ctrl/ILeaseAuthority (:ha-authority m)))
            (dha/refresh-ha-authority-state db-name m)
            m)))))
  (when-let [err (ha-write-admission-error server message)]
    (u/raise "HA write admission rejected" err)))

(defn- ha-write-commit-check-fn
  [^Server server message]
  (fn [_]
    (ha-write-commit-admission! server message)))

(defn- with-ha-write-admission
  [^Server server message f]
  (let [write?  (dha/ha-write-message? message)
        db-name (nth (:args message) 0 nil)
        dbs     (.-dbs server)]
    (if (and write? db-name (.containsKey ^ConcurrentHashMap dbs db-name))
      (locking (db-write-admission-lock server db-name)
        (if-let [err (ha-write-admission-error server message)]
          {:ok? false
           :error err}
          {:ok? true
           :result (f)}))
      {:ok? true
       :result (f)})))

(defn- ensure-ha-runtime
  ([root db-name m store]
   (ensure-ha-runtime root db-name m store nil))
  ([root db-name m store explicit-ha-runtime-opts]
   (hrt/ensure-ha-runtime
    root
    db-name
    m
    store
    explicit-ha-runtime-opts
    {:resolved-ha-runtime-opts-fn resolved-ha-runtime-opts
     :start-ha-authority-fn *start-ha-authority-fn*
     :stop-ha-runtime-fn stop-ha-runtime})))

(defn- add-store
  ([server db-name store]
   (add-store server db-name store true nil))
  ([^Server server db-name store activate-runtime?]
   (add-store server db-name store activate-runtime? nil))
  ([^Server server db-name store activate-runtime? explicit-ha-runtime-opts]
   (letfn [(add-store* [store]
             (let [published-store-v (volatile! store)]
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
               @published-store-v))
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
     (if writing? (:wdt-db m) (:dt-db m)))))

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
  (let [sys-conn    (.-sys-conn server)
        roles       (user-roles sys-conn target-username)
        permissions (user-permissions sys-conn target-username)]
    (doseq [cid (keep (fn [[client-id {:keys [username]}]]
                        (when (= target-username username) client-id))
                      (.-clients server))]
      (update-client server cid
                     #(assoc % :roles roles :permissions permissions)))))

(defn- disconnect-client*
  [^Server server client-id]
  (remove-client server client-id)
  (let [^Selector selector (.-selector server)]
    (when (.isOpen selector)
      (doseq [^SelectionKey k (.keys selector)
              :let            [state (.attachment k)]
              :when           state]
        (when (= client-id (@state :client-id))
          (close-conn k))))))

(defn- disconnect-user
  [^Server server tgt-username]
  (doseq [[client-id {:keys [username]}] (.-clients server)
          :when                          (= tgt-username username)]
    (disconnect-client* server client-id)))

(defn- update-cached-permission
  [^Server server target-role]
  (let [sys-conn (.-sys-conn server)]
    (doseq [[cid uname] (keep (fn [[client-id {:keys [username roles]}]]
                                (when (some #(= % target-role) roles)
                                  [client-id username]))
                              (.-clients server))]
      (update-client server cid
                     #(assoc % :permissions
                             (user-permissions sys-conn uname))))))

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
  (when-let [client-socket (.accept ^ServerSocketChannel (.channel skey))]
    (doto ^SocketChannel client-socket
      (.configureBlocking false)
      (.register (.selector skey) SelectionKey/OP_READ
                 ;; attach a connection state
                 ;; { read-bf, write-bf, client-id }
                 (volatile! {:read-bf  (bf/allocate-buffer
                                         c/+buffer-size+)
                             :write-bf (bf/allocate-buffer
                                         c/+buffer-size+)
                             :wire-opts (p/default-wire-opts)})))))

(defn- copy-in
  "Continuously read batched data from the client"
  [^Server server ^SelectionKey skey]
  (let [state                      (.attachment skey)
        {:keys [read-bf write-bf wire-opts]} @state
        ^Selector selector         (.selector skey)
        ^SocketChannel ch          (.channel skey)
        data                       (transient [])]
    ;; switch this channel to blocking mode for copy-in
    (.cancel skey)
    (.configureBlocking ch true)
    (try
      (p/write-message-blocking ch write-bf {:type :copy-in-response}
                                wire-opts)
      (.clear ^ByteBuffer read-bf)
      (loop [bf read-bf]
        (let [[msg bf'] (p/receive-ch ch bf wire-opts)]
          (when-not (identical? bf bf') (vswap! state assoc :read-bf bf'))
          (if (map? msg)
            (let [{:keys [type]} msg]
              (case type
                :copy-done :break
                :copy-fail (u/raise "Client error while loading data" {})
                (u/raise "Receive unexpected message while loading data"
                         {:msg msg})))
            (do (doseq [d msg] (conj! data d))
                (recur bf')))))
      (let [txs (persistent! data)]
        (log/debug "Copied in" (count txs) "data items")
        txs)
      (catch Exception e (throw e))
      (finally
        ;; switch back
        (.configureBlocking ch false)
        (.add ^ConcurrentLinkedQueue (.-register-queue server)
              [ch SelectionKey/OP_READ state])
        (.wakeup selector)))))

(defn- copy-out
  "Continiously write data out to client in batches"
  ([^SelectionKey skey data batch-size]
   (copy-out skey data batch-size nil nil))
  ([^SelectionKey skey data batch-size copy-meta]
   (copy-out skey data batch-size copy-meta nil))
  ([^SelectionKey skey data batch-size copy-meta response-meta]
   (let [state                             (.attachment skey)
         {:keys [^ByteBuffer write-bf wire-opts]}    @state
         ^SocketChannel                 ch (.channel skey)
         response                          (cond-> {:type :copy-out-response}
                                             copy-meta
                                             (assoc :copy-meta copy-meta)
                                             (seq response-meta)
                                             (merge response-meta))]
     (locking write-bf
       (p/write-message-blocking ch write-bf response wire-opts)
       (doseq [batch (partition batch-size batch-size nil data)]
         (write-message skey batch))
       (p/write-message-blocking ch write-bf {:type :copy-done}
                                 wire-opts))
     (log/debug "Copied out" (count data) "data items"))))

(defn- copy-file-out
  "Stream a copied LMDB file to client as raw binary chunks with checksum."
  [^SelectionKey skey path copy-meta]
  (let [chunk-bytes ^long c/+buffer-size+
        response    (cond-> {:type          :copy-out-response
                             :copy-format   :binary-chunks
                             :checksum-algo :sha-256
                             :chunk-bytes   chunk-bytes}
                      copy-meta
                      (assoc :copy-meta copy-meta))
        ^MessageDigest md (MessageDigest/getInstance "SHA-256")
        chunk             (byte-array chunk-bytes)]
    (write-message skey response)
    (with-open [in (Files/newInputStream path
                                         (into-array OpenOption []))]
      (loop [written-bytes 0
             chunk-count   0]
        (let [n (.read in chunk)]
          (if (neg? n)
            (let [checksum (u/hexify (.digest md))]
              (write-message skey {:type          :copy-done
                                   :copy-format   :binary-chunks
                                   :checksum-algo :sha-256
                                   :checksum      checksum
                                   :bytes         written-bytes
                                   :chunks        chunk-count})
              (log/debug "Copied out" written-bytes "bytes in" chunk-count
                         "chunks"))
            (let [^bytes out-chunk (if (= n chunk-bytes)
                                     chunk
                                     (let [tail (byte-array n)]
                                       (System/arraycopy chunk 0 tail 0 n)
                                       tail))]
              (.update md out-chunk 0 n)
              (write-message skey [out-chunk])
              (recur (+ written-bytes n) (inc chunk-count)))))))))

(defn- copy-source-kv-store
  [store]
  (cond
    (instance? Store store) (.-lmdb ^Store store)
    (instance? ILMDB store) store
    :else nil))

(defn- copy-response-meta
  [db-name store base-meta]
  (let [store-opts (when (instance? IStore store)
                     (i/opts store))
        kv-store   (copy-source-kv-store store)
        kv-opts    (when kv-store
                     (try
                       (i/env-opts kv-store)
                       (catch Exception _
                         nil)))
        stored-db-identity
        (when kv-store
          (try
            (i/get-value kv-store c/opts :db-identity :attr :data)
            (catch Exception _
              nil)))
        snapshot-lsn
        (when kv-store
          (try
            (long (or (i/get-value kv-store c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            (catch Exception _
              0)))
        payload-lsn
        (when kv-store
          (try
            (long (dha/read-ha-snapshot-payload-lsn {:store store}))
            (catch Exception _
              0)))
        db-identity (or (:db-identity store-opts)
                        (:db-identity kv-opts)
                        stored-db-identity)]
    (cond-> (assoc base-meta :db-name db-name)
      (some? db-identity)
      (assoc :db-identity db-identity)

      (some? snapshot-lsn)
      (assoc :snapshot-last-applied-lsn (long snapshot-lsn))

      (some? payload-lsn)
      (assoc :payload-last-applied-lsn (long payload-lsn)))))

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
  (boolean
    (some
      (fn [cause]
        (let [message (ex-message cause)]
          (or (instance? ClosedChannelException cause)
              (= message "Socket channel is closed.")
              (and (string? message)
                   (or (s/includes? message "Connection reset by peer")
                       (s/includes? message "Broken pipe"))))))
      (take-while some? (iterate ex-cause e)))))

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
  (let [data (ex-data e)]
    (cond
      (client-disconnect? e)
      (close-conn-quietly skey)

      (= (:type data) :reopen)
      (try
        (reopen-response skey data)
        (catch Exception reopen-e
          (when-not (client-disconnect? reopen-e)
            (log/error reopen-e "Failed to send reopen response"))
          (close-conn-quietly skey)))

      :else
      (do
        (log/error e)
        (try
          (error-response skey (ex-message e) data)
          (catch Exception response-e
            (when-not (client-disconnect? response-e)
              (log/error response-e "Failed to send error response"))
            (close-conn-quietly skey)))))))

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

(defn- new-search-engine
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[db-name opts]      args
          {:keys [client-id]} @(.attachment skey)
          engine              (or (search-engine* server skey db-name)
                                  (if-let [store (get-store server db-name)]
                                    (sc/new-search-engine store opts)
                                    (u/raise "engine store not found"
                                             {::type   :reopen
                                              :db-name db-name
                                              :db-type "kv"})))]
      (update-client server client-id #(update % :engines conj db-name))
      (update-db server db-name #(assoc % :engine engine))
      (write-message skey {:type :command-complete}))))

(defmacro search-handler
  "Handle request to search engine"
  [f]
  `(write-message
     ~'skey
     {:type   :command-complete
      :result (apply
                ~(symbol "datalevin.interface" (str f))
                (search-engine ~'server ~'skey (nth ~'args 0))
                (rest ~'args))}))

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

(defn- new-vector-index
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[db-name opts]      args
          {:keys [client-id]} @(.attachment skey)
          index               (if-let [store (get-store server db-name)]
                                (v/new-vector-index store opts)
                                (u/raise "vector store not found"
                                         {::type   :reopen
                                          :db-name db-name
                                          :db-type "kv"}))]
      (update-client server client-id #(update % :indices conj db-name))
      (update-db server db-name #(assoc % :index index))
      (write-message skey {:type :command-complete}))))

(defmacro vector-handler
  "Handle request to vector index"
  [f]
  `(write-message
     ~'skey
     {:type   :command-complete
      :result (apply
                ~(symbol "datalevin.interface" (str f))
                (vector-index ~'server ~'skey (nth ~'args 0))
                (rest ~'args))}))

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
      ;; Preserve the legacy remote open-with-schema behavior for plain stores,
      ;; but never synthesize follower-local schema rows on HA databases.
      (when (and schema
                 (nil? (:ha-mode (i/opts store))))
        (i/set-schema store schema))
      store)

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
  (let [lock-v (volatile! nil)]
    (update-db server db-name
               (fn [m]
                 (let [lock (or (:open-lock m) (Object.))]
                   (vreset! lock-v lock)
                   (assoc m :open-lock lock))))
    @lock-v))

(defn- db-write-admission-lock
  [^Server server db-name]
  (let [lock-v (volatile! nil)]
    (update-db server db-name
               (fn [m]
                 (let [lock (or (:ha-write-admission-lock m) (Object.))]
                   (vreset! lock-v lock)
                   (assoc m :ha-write-admission-lock lock))))
    @lock-v))

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
                 store            (add-store
                                    server db-name store activate-runtime? opts)
                 datalog?         (instance? Store store)]
            (update-client server client-id
                           #(cond-> %
                              true     (update :stores assoc db-name
                                               {:datalog? datalog?
                                                :dbis     #{}})
                              (and datalog? activate-runtime?)
                              (update :dt-dbs conj db-name)))
            (when-not existing-db-now?
              (transact-new-db sys-conn username db-type db-name)
              (update-client server client-id
                             #(assoc % :permissions
                                     (user-permissions sys-conn username))))
            (let [db-info (when (and return-db-info? datalog?)
                            {:max-eid       (i/init-max-eid store)
                             :max-tx        (i/max-tx store)
                             :last-modified (i/last-modified store)
                             :opts          (i/opts store)})]
              (when respond?
                (write-message skey
                               (cond-> {:type :command-complete}
                                 db-info (assoc :result db-info))))
              db-info)))))))

(defn- session-lmdb [sys-conn] (.-lmdb ^Store (.-store ^DB (d/db sys-conn))))

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
  (let [lmdb (session-lmdb sys-conn)]
    (d/open-dbi lmdb session-dbi)
    (ConcurrentHashMap.
      ^Map (into {} (d/get-range lmdb session-dbi [:all] :uuid :data)))))

(defn- reopen-dbs
  [root clients ^ConcurrentHashMap dbs]
  (doseq [[_ {:keys [stores engines indices dt-dbs]}] clients]
    (doseq [[db-name {:keys [datalog? dbis]}]
            stores
            :when (not (get-in dbs [db-name :store]))
            :let  [m (get dbs db-name {})]]
      (let [store (open-store root db-name dbis datalog?)
            consensus-ha? (and datalog?
                               (some? (*consensus-ha-opts-fn* store)))]
        (if consensus-ha?
          (do
            ;; Consensus HA runtime identity is node-local. Restoring a DB from
            ;; persisted client sessions before a fresh explicit open can start
            ;; the wrong peer from stale store metadata after restart.
            (close-store store)
            (log/info "Skipping automatic reopen of consensus HA database"
                      {:db-name db-name
                       :root root}))
          (let [runtime-opts (resolved-runtime-opts nil db-name store m)
                next-m (ensure-ha-runtime
                         root db-name
                         (cond-> (assoc m
                                        :store store
                                        :runtime-opts runtime-opts)
                           datalog?
                           (assoc :dt-db (new-runtime-db store runtime-opts)))
                         store)]
            (.put dbs db-name next-m)))))
    (doseq [db-name engines
            :when   (and (not (get-in dbs [db-name :engine]))
                         (get-in dbs [db-name :store]))
            :let    [m (get dbs db-name {})]]
      (.put dbs db-name
            (assoc m :engine
                   (d/new-search-engine (get-in dbs [db-name :store])))))
    (doseq [db-name indices
            :when   (and (not (get-in dbs [db-name :index]))
                         (get-in dbs [db-name :store]))
            :let    [m (get dbs db-name {})]]
      (.put dbs db-name
            (assoc m :index
                   (d/new-vector-index (get-in dbs [db-name :store])))))
    (doseq [db-name dt-dbs
            :when   (and (not (get-in dbs [db-name :dt-db]))
                         (get-in dbs [db-name :store]))
            :let    [m (get dbs db-name {})]]
      (.put dbs db-name
            (assoc m :dt-db
                   (new-runtime-db (get-in dbs [db-name :store])
                                   (current-runtime-opts m)))))))

(defn- authenticate
  [^Server server ^SelectionKey skey {:keys [username password]}]
  (when-let [{:keys [user/pw-salt user/pw-hash]}
             (pull-user (.-sys-conn server) username)]
    (when (password-matches? password pw-hash pw-salt)
      (let [client-id (UUID/randomUUID)
            ip        (get-ip skey)]
        (add-client server ip client-id username)
        client-id))))

(defn- client-display
  [^Server server [client-id m]]
  (let [sys-conn (.-sys-conn server)]
    [client-id
     (-> m
         (update :permissions
                 #(mapv
                    (fn [{:keys [permission/act permission/obj
                                permission/tgt]}]
                      (if-let [{:keys [db/id]} tgt]
                        [act obj (perm-tgt-name sys-conn obj id)]
                        [act obj]))
                    %))
         (assoc :open-dbs (:stores m))
         (select-keys [:ip :username :roles :permissions :open-dbs]))]))

;; BEGIN message handlers

(def message-handlers
  ['authentication
   'disconnect
   'set-client-id
   'create-user
   'reset-password
   'drop-user
   'list-users
   'create-role
   'drop-role
   'list-roles
   'create-database
   'close-database
   'drop-database
   'list-databases
   'list-databases-in-use
   'assign-role
   'withdraw-role
   'list-user-roles
   'grant-permission
   'revoke-permission
   'list-role-permissions
   'list-user-permissions
   'query-system
   'show-clients
   'disconnect-client
   'open
   'close
   'closed?
   'opts
   'assoc-opt
   'last-modified
   'schema
   'rschema
   'set-schema
   'init-max-eid
   'max-tx
   'swap-attr
   'del-attr
   'rename-attr
   'datom-count
   'load-datoms
   'tx-data
   'db-info
   'tx-data+db-info
   'open-transact
   'close-transact
   'abort-transact
   'set-env-flags
   'get-env-flags
   'sync
   'ha-watermark
   'txlog-watermarks
   'open-tx-log
   'open-tx-log-rows
   'read-commit-marker
   'verify-commit-marker!
   'force-txlog-sync!
   'force-lmdb-sync!
   'create-snapshot!
   'list-snapshots
   'snapshot-scheduler-state
   'txlog-retention-state
   'gc-txlog-segments!
   'txlog-update-snapshot-floor!
   'txlog-clear-snapshot-floor!
   'txlog-update-replica-floor!
   'txlog-clear-replica-floor!
   'txlog-pin-backup-floor!
   'txlog-unpin-backup-floor!
   'fetch
   'populated?
   'size
   'head
   'tail
   'slice
   'rslice
   'start-sampling
   'stop-sampling
   'analyze
   'e-datoms
   'e-first-datom
   'av-datoms
   'av-first-datom
   'av-first-e
   'ea-first-datom
   'ea-first-v
   'v-datoms
   'size-filter
   'head-filter
   'tail-filter
   'slice-filter
   'rslice-filter
   'open-kv
   'close-kv
   'closed-kv?
   'open-dbi
   'clear-dbi
   'drop-dbi
   'list-dbis
   'copy
   'stat
   'entries
   'open-transact-kv
   'close-transact-kv
   'abort-transact-kv
   'transact-kv
   'get-value
   'get-rank
   'get-by-rank
   'sample-kv
   'get-first
   'get-first-n
   'batch-kv
   'key-range
   'key-range-count
   'key-range-list-count
   'visit-key-range
   'get-range
   'range-count
   'get-some
   'range-filter
   'range-keep
   'range-some
   'range-filter-count
   'visit
   'get-list
   'visit-list
   'list-count
   'in-list?
   'list-range
   'list-range-count
   'list-range-first
   'list-range-first-n
   'list-range-filter
   'list-range-some
   'list-range-keep
   'list-range-filter-count
   'visit-list-range
   'q
   'pull
   'pull-many
   'explain
   'fulltext-datoms
   'new-search-engine
   'add-doc
   'remove-doc
   'clear-docs
   'doc-indexed?
   'doc-count
   'search
   'search-re-index
   'new-vector-index
   'add-vec
   'remove-vec
   'persist-vecs
   'close-vecs
   'clear-vecs
   'vecs-info
   'vec-indexed?
   'search-vec
   'vec-re-index
   'kv-re-index
   'datalog-re-index
   ])

(defmacro message-cases
  "Message handler function should have the same name as the incoming message
  type, e.g. '(authentication skey message) for :authentication message type"
  [skey type]
  `(case ~type
     ~@(mapcat
         (fn [sym]
           [(keyword sym) (list sym 'server 'skey 'message)])
         message-handlers)
     (error-response ~skey (str "Unknown message type " ~type))))

(defn- authentication
  [^Server server skey message]
  (wrap-error
    (if-let [client-id (authenticate server skey message)]
      (write-message skey {:type :authentication-ok :client-id client-id})
      (u/raise "Failed to authenticate" {}))))

(defn- disconnect
  [server ^SelectionKey skey _]
  (let [{:keys [client-id]} @(.attachment skey)]
    (disconnect-client* server client-id)))

(defn- set-client-id
  [^Server server ^SelectionKey skey message]
  (let [client-id         (message :client-id)
        wire-capabilities (:wire-capabilities message)
        wire-opts         (p/negotiate-wire-opts wire-capabilities)]
    ;; Respond in the legacy raw format, then enable negotiated wire options.
    (write-message skey {:type              :set-client-id-ok
                         :wire-capabilities (p/local-wire-capabilities)})
    (vswap! (.attachment skey)
            assoc :client-id client-id :wire-opts wire-opts)))

(defn- create-user
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [username password] args
          username            (u/lisp-case username)]
      (wrap-permission
        ::create ::user nil
        "Don't have permission to create user"
        (if (s/blank? password)
          (u/raise "Password is required when creating user." {})
          (do (transact-new-user sys-conn username password)
              (write-message skey {:type     :command-complete
                                   :username username})))))))

(defn- reset-password
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [username password] args
          uid                 (user-eid sys-conn username)]
      (if uid
        (wrap-permission
          ::alter ::user uid
          (str "Don't have permission to reset password of " username)
          (if (s/blank? password)
            (u/raise "New password is required when resetting password" {})
            (do (transact-new-password sys-conn username password)
                (write-message skey {:type :command-complete}))))
        (u/raise "User does not exist" {:username username})))))

(defn- drop-user
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn   (.-sys-conn server)
          [username] args
          uid        (user-eid sys-conn username)]
      (if (= username c/default-username)
        (u/raise "Default user cannot be dropped." {})
        (if uid
          (wrap-permission
            ::create ::user uid
            "Don't have permission to drop the user"
            (disconnect-user server username)
            (transact-drop-user sys-conn uid username)
            (write-message skey {:type :command-complete}))
          (u/raise "User does not exist." {:user username}))))))

(defn- list-users
  [^Server server ^SelectionKey skey _]
  (wrap-error
    (wrap-permission
      ::view ::user nil
      "Don't have permission to list users"
      (write-message skey {:type   :command-complete
                           :result (query-users (.-sys-conn server))}))))

(defn- create-role
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[role-key] args]
      (wrap-permission
        ::create ::role nil
        "Don't have permission to create role"
        (transact-new-role (.-sys-conn server) role-key)
        (write-message skey {:type :command-complete})))))

(defn- drop-role
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn   (.-sys-conn server)
          [role-key] args
          rid        (role-eid sys-conn role-key)]
      (if rid
        (if (user-role-key? sys-conn role-key)
          (u/raise "Cannot drop default role of an active user" {})
          (wrap-permission
            ::create ::role rid
            "Don't have permission to drop the role"
            (transact-drop-role sys-conn rid)
            (update-cached-permission server role-key)
            (write-message skey {:type :command-complete})))
        (u/raise "Role does not exist." {:role role-key})))))

(defn- list-roles
  [^Server server ^SelectionKey skey _]
  (wrap-error
    (wrap-permission
      ::view ::role nil
      "Don't have permission to list roles"
      (write-message skey {:type   :command-complete
                           :result (query-roles (.-sys-conn server))}))))

(defn- create-database
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[db-name db-type] args
          db-name           (u/lisp-case db-name)]
      (wrap-permission
        ::create ::database nil
        "Don't have permission to create database"
        (if (db-exists? server db-name)
          (u/raise "Database already exists." {:db db-name})
          (do
            (open-server-store server skey
                               {:db-name db-name
                                :respond? false} db-type)
            nil))
        (write-message skey {:type :command-complete})))))

(defn- close-database
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [db-name]           args
          {:keys [client-id]} @(.attachment skey)
          did                 (db-eid sys-conn db-name)]
      (if did
        (if (get-store server db-name)
          (wrap-permission
            ::create ::database did
            "Don't have permission to close the database"
            (doseq [[cid {:keys [stores]}] (.-clients server)
                    :when                  (get stores db-name)]
              (when (not= client-id cid)
                (disconnect-client* server cid)))
            (remove-store server db-name)
            (write-message skey {:type :command-complete}))
          (u/raise "Database is closed already." {}))
        (u/raise "Database doe snot exist." {})))))

(defn- drop-database
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn  (.-sys-conn server)
          [db-name] args
          did       (db-eid sys-conn db-name)]
      (if did
        (wrap-permission
          ::create ::database did
          "Don't have permission to drop the database"
          (if (db-in-use? server db-name)
            (u/raise "Cannot drop a database currently in use." {})
            (do (transact-drop-db sys-conn did)
                (u/delete-files (db-dir (.-root server) db-name))
                (write-message skey {:type :command-complete}))))
        (u/raise "Database does not exist." {})))))

(defn- list-databases
  [^Server server ^SelectionKey skey _]
  (wrap-error
    (wrap-permission
      ::create ::database nil
      "Don't have permission to list databases"
      (write-message skey {:type   :command-complete
                           :result (query-databases (.-sys-conn server))}))))

(defn- list-databases-in-use
  [^Server server ^SelectionKey skey _]
  (wrap-error
    (wrap-permission
      ::create ::database nil
      "Don't have permission to list databases in use"
      (write-message skey {:type   :command-complete
                           :result (in-use-dbs server)}))))

(defn- assign-role
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [role-key username] args
          rid                 (role-eid sys-conn role-key)]
      (if rid
        (wrap-permission
          ::alter ::role rid
          "Don't have permission to assign the role to user"
          (transact-user-role sys-conn rid username)
          (update-cached-role server username)
          (write-message skey {:type :command-complete}))
        (u/raise "Role does not exist." {})))))

(defn- withdraw-role
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [role-key username] args
          rid                 (role-eid sys-conn role-key)]
      (if rid
        (if (user-role-key? sys-conn role-key username)
          (u/raise "Cannot withdraw the default role of a user" {})
          (wrap-permission
            ::alter ::role rid
            "Don't have permission to withdraw the role from user"
            (transact-withdraw-role sys-conn rid username)
            (update-cached-role server username)
            (write-message skey {:type :command-complete})))
        (u/raise "Role does not exist." {})))))

(defn- list-user-roles
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn   (.-sys-conn server)
          [username] args
          uid        (user-eid sys-conn username)]
      (if uid
        (wrap-permission
          ::view ::user uid
          "Don't have permission to view the user's roles"
          (write-message skey {:type   :command-complete
                               :result (user-roles sys-conn username)}))
        (u/raise "User does not exist." {})))))

(defn- grant-permission
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn                              (.-sys-conn server)
          [role-key perm-act perm-obj perm-tgt] args
          rid                                   (role-eid sys-conn role-key)]
      (if rid
        (wrap-permission
          ::alter ::role rid
          "Don't have permission to grant permission to the role"
          (if (and (permission-actions perm-act) (permission-objects perm-obj))
            (transact-role-permission sys-conn rid perm-act perm-obj perm-tgt)
            (u/raise "Unknown permission action or object." {}))
          (update-cached-permission server role-key)
          (write-message skey {:type :command-complete}))
        (u/raise "Role does not exist." {})))))

(defn- revoke-permission
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn                              (.-sys-conn server)
          [role-key perm-act perm-obj perm-tgt] args
          rid                                   (role-eid sys-conn role-key)]
      (if rid
        (wrap-permission
          ::alter ::role rid
          "Don't have permission to revoke permission from the role"
          (transact-revoke-permission sys-conn rid perm-act perm-obj perm-tgt)
          (update-cached-permission server role-key)
          (write-message skey {:type :command-complete}))
        (u/raise "Role does not exist." {})))))

(defn- list-role-permissions
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn   (.-sys-conn server)
          [role-key] args
          rid        (role-eid sys-conn role-key)]
      (if rid
        (wrap-permission
          ::view ::role rid
          "Don't have permission to list permissions of the role"
          (write-message skey {:type   :command-complete
                               :result (role-permissions sys-conn role-key)}))
        (u/raise "Role does not exist." {})))))

(defn- list-user-permissions
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn   (.-sys-conn server)
          [username] args
          uid        (user-eid sys-conn username)]
      (if uid
        (wrap-permission
          ::view ::user uid
          "Don't have permission to list permission of the user"
          (write-message skey {:type   :command-complete
                               :result (user-permissions sys-conn username)}))
        (u/raise "User does not exist." {})))))

(defn- query-system
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[query arguments] args]
      (wrap-permission
        ::view ::server nil
        "Don't have permission to query system."
        (write-message skey {:type   :command-complete
                             :result (apply d/q query
                                            @(.-sys-conn server)
                                            arguments)})))))
(defn- show-clients
  [^Server server ^SelectionKey skey _]
  (wrap-error
    (wrap-permission
      ::view ::server nil
      "Don't have permission to show clients."
      (write-message skey
                     {:type   :command-complete
                      :result (->> (.-clients server)
                                   (map (partial client-display server))
                                   (into {}))}))))

(defn- disconnect-client
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[cid] args]
      (wrap-permission
        ::control ::server nil
        "Don't have permission to disconnect a client"
        (disconnect-client* server cid)
        (write-message skey {:type :command-complete})))))

(defn- open
  "Open a datalog store."
  [^Server server ^SelectionKey skey message]
  (open-server-store server skey message c/dl-type))

(defn- close
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (detach-client-store! server skey (nth args 0))
    (write-message skey {:type :command-complete})))

(defn- closed?
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          res     (if-let [s (store server skey db-name writing?)]
                    (store-closed? s)
                    true)]
      (write-message skey {:type :command-complete :result res}))))

(defn- opts
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler opts)))

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

(defn- assoc-opt
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          store   (store server skey db-name writing?)
          [k v]   (rest args)
          result  (apply-assoc-opt! server db-name store writing? k v)]
      (write-message skey {:type :command-complete
                           :result result}))))

(defn- last-modified
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler last-modified)))

(defn- schema
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler schema)))

(defn- rschema
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler rschema)))

(defn- set-schema
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (wrap-permission
      ::alter ::database (db-eid (.-sys-conn server)
                                 (store->db-name
                                   server
                                   (db-store server skey (nth args 0))))
      "Don't have permission to alter the database"
      (normal-dt-store-handler set-schema))))

(defn- init-max-eid
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler init-max-eid)))

(defn- max-tx
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler max-tx)))

(defn- swap-attr
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-dt-store-handler swap-attr))))

(defn- del-attr
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler del-attr)))

(defn- rename-attr
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler rename-attr)))

(defn- datom-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler datom-count)))

(defn- load-datoms
  [^Server server ^SelectionKey skey {:keys [mode args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (case mode
          :copy-in (let [dt-store (store server skey db-name writing?)]
                     (i/load-datoms dt-store (copy-in server skey))
                     (write-message skey {:type :command-complete}))
          :request (normal-dt-store-handler load-datoms)
          (u/raise "Missing :mode when loading datoms" {}))))))

(defn- transact*
  [db txs s? server db-name writing?]
  (try
    (d/with db txs {} s?)
    (catch Exception e
      (when (:resized (ex-data e))
        (let [^DB new-db (db/carry-runtime-opts
                           (db/new-db (get-store server db-name writing?))
                           db)]
          (update-db server db-name
                     #(assoc % (if writing? :wdt-db :dt-db)
                             new-db))))
      (throw e))))

(defn- tx-data
  [^Server server ^SelectionKey skey {:keys [mode args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (let [txs (case mode
                    :copy-in (copy-in server skey)
                    :request (nth args 1)
                    (u/raise "Missing :mode when transact data" {}))
              db  (get-db server db-name writing?)
              s?  (last args)
              rp  (transact* db txs s? server db-name writing?)
              db  (:db-after rp)
              _   (update-db server db-name
                             #(assoc % (if writing? :wdt-db :dt-db) db))
              rp  (assoc-in rp [:tempids :max-eid] (:max-eid db))
              ct  (+ (count (:tx-data rp)) (count (:tempids rp)))
              res (cond-> (select-keys rp [:tx-data :tempids])
                    (:new-attributes rp)
                    (assoc :new-attributes (:new-attributes rp)))]
          (if (< ct ^long c/+wire-datom-batch-size+)
            (write-message skey {:type :command-complete :result res})
            (let [{:keys [tx-data tempids]} res
                  response-meta            (dissoc res :tx-data :tempids)]
              (copy-out skey (into tx-data tempids)
                        c/+wire-datom-batch-size+
                        nil response-meta))))))))

(defn- fetch
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler fetch)))

(defn- populated?
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler populated?)))

(defn- size
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler size)))

(defn- head
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler head)))

(defn- tail
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler tail)))

(defn- slice
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          datoms  (apply i/slice
                         (store server skey db-name writing?)
                         (rest args))]
      (if (< (count datoms) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- rslice
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          datoms  (apply i/rslice
                         (store server skey db-name writing?)
                         (rest args))]
      (if (< (count datoms) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- db-info
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          dt-store (store server skey db-name writing?)]
      (write-message skey {:type   :command-complete
                           :result {:max-eid       (i/init-max-eid dt-store)
                                    :max-tx        (i/max-tx dt-store)
                                    :last-modified (i/last-modified dt-store)
                                    :opts          (i/opts dt-store)}}))))

(defn- tx-data+db-info
  [^Server server ^SelectionKey skey {:keys [mode args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (let [txs (case mode
                    :copy-in (copy-in server skey)
                    :request (nth args 1)
                    (u/raise "Missing :mode when transact data" {}))
              db  (get-db server db-name writing?)
              s?  (last args)
              rp  (transact* db txs s? server db-name writing?)
              db  (:db-after rp)
              _   (update-db server db-name
                             #(assoc % (if writing? :wdt-db :dt-db) db))
              rp  (assoc-in rp [:tempids :max-eid] (:max-eid db))
              dt-store (store server skey db-name writing?)
              db-info  {:max-eid       (:max-eid db)
                        :max-tx        (i/max-tx dt-store)
                        :last-modified (i/last-modified dt-store)}
              ct  (+ (count (:tx-data rp)) (count (:tempids rp)))
              res (cond-> (select-keys rp [:tx-data :tempids])
                    (:new-attributes rp)
                    (assoc :new-attributes (:new-attributes rp))
                    true
                    (assoc :db-info db-info))]
          (if (< ct ^long c/+wire-datom-batch-size+)
            (write-message skey {:type :command-complete :result res})
            (let [{:keys [tx-data tempids]} res
                  response-meta            (dissoc res :tx-data :tempids)]
              (copy-out skey (into tx-data tempids)
                        c/+wire-datom-batch-size+
                        nil response-meta))))))))

(defn- start-sampling
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler start-sampling)))

(defn- stop-sampling
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler stop-sampling)))

(defn- analyze
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler analyze)))

(defn- e-datoms
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          datoms  (apply i/e-datoms
                         (store server skey db-name writing?)
                         (rest args))]
      (if (< (count datoms) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- e-first-datom
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler e-first-datom)))

(defn- av-datoms
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          datoms  (apply i/av-datoms
                         (store server skey db-name writing?)
                         (rest args))]
      (if (< (count datoms) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- av-first-datom
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler av-first-datom)))

(defn- av-first-e
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler av-first-e)))

(defn- ea-first-datom
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler ea-first-datom)))

(defn- ea-first-v
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler ea-first-v)))

(defn- v-datoms
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          datoms  (apply i/v-datoms
                         (store server skey db-name writing?)
                         (rest args))]
      (if (< (count datoms) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- size-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-dt-store-handler size-filter))))

(defn- head-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-dt-store-handler head-filter))))

(defn- tail-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-dt-store-handler tail-filter))))

(defn- slice-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)
          datoms (apply i/slice-filter
                        (store server skey (nth args 0) writing?)
                        (rest args))]
      (if (< (count datoms) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- rslice-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)
          datoms (apply i/rslice-filter
                        (store server skey (nth args 0) writing?)
                        (rest args))]
      (if (< (count datoms) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- open-kv
  [^Server server ^SelectionKey skey message]
  (open-server-store server skey message c/kv-type))

(defn- close-kv
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (detach-client-store! server skey (nth args 0))
    (write-message skey {:type :command-complete})))

(defn- closed-kv?
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler closed-kv?)))

(defn- open-dbi
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [{:keys [client-id]} @(.attachment skey)
          db-name             (nth args 0)
          kv                  (lmdb server skey db-name writing?)
          args                (rest args)
          dbi-name            (first args)]
      (apply i/open-dbi kv args)
      (update-client server client-id
                     #(update-in % [:stores db-name :dbis] conj dbi-name)))
    (write-message skey {:type :command-complete})))

(defn- clear-dbi
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler clear-dbi)))

(defn- drop-dbi
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [{:keys [client-id]} @(.attachment skey)
          db-name             (nth args 0)
          kv                  (lmdb server skey db-name writing?)
          args                (rest args)
          dbi-name            (first args)]
      (i/drop-dbi kv dbi-name)
      (update-client server client-id
                     #(update-in % [:stores db-name :dbis] disj dbi-name)))
    (write-message skey {:type :command-complete})))

(defn- list-dbis
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler list-dbis)))

(defn- cleanup-copy-tmp-dir*
  [tf]
  (u/delete-files tf))

(def ^:private ^:redef cleanup-copy-tmp-dir-fn*
  (atom cleanup-copy-tmp-dir*))

(defn- cleanup-copy-tmp-dir!
  [tf]
  (@cleanup-copy-tmp-dir-fn* tf))

(def ^:private ^:redef server-copy-store!
  i/copy)

(def ^:private ^:redef open-server-copied-store!
  st/open)

(def ^:private ^:redef close-server-copied-store!
  i/close)

(def ^:private ^:redef copy-server-file-out!
  copy-file-out)

(def ^:private ^:redef unpin-server-copy-backup-floor!
  kv/txlog-unpin-backup-floor!)

(defn- best-effort-unpin-server-copy-backup-floor!
  [db-name source-store copy-backup-pin]
  (when-let [pin-id (:pin-id copy-backup-pin)]
    (try
      (unpin-server-copy-backup-floor! source-store pin-id)
      (catch Throwable e
        (log/debug e
                   "Best-effort server copy backup pin cleanup failed"
                   {:db-name db-name
                    :pin-id pin-id})))))

(defn- copy
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [[db-name compact?] args
          source-store        (lmdb server skey db-name writing?)
          started-ms          (System/currentTimeMillis)
          copy-backup-pin     (atom nil)
          tf                 (u/tmp-dir (str "copy-" (UUID/randomUUID)))
          path               (Paths/get (str tf u/+separator+ c/data-file-name)
                                        (into-array String []))]
      (try
        (binding [kv/*wal-copy-backup-pin-observer*
                  (fn [{:keys [pin-id pin-floor-lsn pin-expires-ms]}]
                    (reset! copy-backup-pin
                            {:pin-id pin-id
                             :floor-lsn pin-floor-lsn
                             :expires-ms pin-expires-ms}))]
          (server-copy-store! source-store tf compact?))
        (let [completed-ms (System/currentTimeMillis)
              copied-store (open-server-copied-store! tf nil nil)]
          (try
            (let [copy-meta (copy-response-meta
                             db-name
                             copied-store
                             (cond-> {:started-ms started-ms
                                      :completed-ms completed-ms
                                      :duration-ms (- completed-ms started-ms)
                                      :compact? (boolean compact?)}
                               (map? @copy-backup-pin)
                               (assoc :backup-pin @copy-backup-pin)))]
              (copy-server-file-out! skey path copy-meta))
            (finally
              (when-not (i/closed? copied-store)
                (close-server-copied-store! copied-store)))))
        (finally
          (best-effort-unpin-server-copy-backup-floor!
           db-name
           source-store
           @copy-backup-pin)
          (try
            (cleanup-copy-tmp-dir! tf)
            (catch Throwable e
              (log/warn e
                        "Unable to delete temporary copy directory"
                        {:path (str tf)}))))))))

(defn- stat
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler stat)))

(defn- entries
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler entries)))

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

(defn- with-db-runtime-read-access
  [^Server server message f]
  (let [db-name (nth (:args message) 0 nil)
        dbs (.-dbs server)]
    (if (and db-name (.containsKey ^ConcurrentHashMap dbs db-name))
      (let [^ReentrantReadWriteLock lock
            (get-runtime-access-lock server db-name)
            read-lock (.readLock lock)]
        (.lock read-lock)
        (try
          (f)
          (finally
            (.unlock read-lock))))
      (f))))

(defn- with-db-runtime-store-swap
  [^Server server db-name f]
  (if db-name
    (let [^ReentrantReadWriteLock lock
          (get-runtime-access-lock server db-name)
          write-lock (.writeLock lock)]
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

(declare write-txn-runner run-calls halt-run)

(defn- open-transact-kv
  [^Server server ^SelectionKey skey {:keys [args] :as message}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (.acquire ^Semaphore (get-lock server db-name))
        (try
          (let [{:keys [kv-store wlmdb]}
                (open-write-txn-with-retry server db-name)]
            (update-db server db-name
                       #(assoc % :wlmdb wlmdb))
            (let [runner (write-txn-runner server db-name kv-store)]
              (write-message skey {:type :command-complete})
              (run-calls runner)))
          (catch Throwable t
            (.release ^Semaphore (get-in (.-dbs server) [db-name :lock]))
            (throw t)))))))

(defn- close-transact-kv
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)
          dbs      (.-dbs server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (try
          (i/close-transact-kv kv-store)
          (write-message skey {:type :command-complete})
          (finally
            (halt-run (get-in dbs [db-name :runner]))
            (update-db server db-name #(dissoc % :runner :wlmdb))
            (.release ^Semaphore (get-in dbs [db-name :lock]))))))))

(defn- abort-transact-kv
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)
          dbs      (.-dbs server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (try
          (i/abort-transact-kv kv-store)
          (i/close-transact-kv kv-store)
          (finally
            (halt-run (get-in dbs [db-name :runner]))
            (update-db server db-name #(dissoc % :runner :wlmdb))
            (.release ^Semaphore (get-in dbs [db-name :lock]))))
        (write-message skey {:type :command-complete})))))

(defn- open-transact
  [^Server server ^SelectionKey skey {:keys [args] :as message}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (.acquire ^Semaphore (get-lock server db-name))
        (try
          (let [{:keys [store kv-store wlmdb]}
                (open-write-txn-with-retry server db-name)
                wstore (st/transfer store wlmdb)
                runner (write-txn-runner server db-name kv-store)]
            (update-db
              server db-name #(assoc %
                                     :wlmdb wlmdb
                                     :wstore wstore
                                     :wdt-db (new-runtime-db
                                               wstore
                                               (current-runtime-opts %))))
            (write-message skey {:type :command-complete})
            (run-calls runner))
          (catch Throwable t
            (.release ^Semaphore (get-in (.-dbs server) [db-name :lock]))
            (throw t)))))))

(defn- close-transact
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)
          dbs      (.-dbs server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (try
          (i/close-transact-kv kv-store)
          (add-store
            server db-name
            (st/transfer (get-in dbs [db-name :wstore]) kv-store))
          (write-message skey {:type :command-complete})
          (finally
            (halt-run (get-in dbs [db-name :runner]))
            (update-db
              server db-name
              #(dissoc % :wlmdb :wstore :wdt-db :runner))
            (.release ^Semaphore (get-in dbs [db-name :lock]))))))))

(defn- abort-transact
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)
          dbs      (.-dbs server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (try
          (i/abort-transact-kv kv-store)
          (i/close-transact-kv kv-store)
          (finally
            (halt-run (get-in dbs [db-name :runner]))
            (update-db server db-name
                       #(dissoc % :wlmdb :wstore :wdt-db :runner))
            (.release ^Semaphore (get-in dbs [db-name :lock]))))
        (write-message skey {:type :command-complete})))))

(defn- get-env-flags
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler get-env-flags)))

(defn- set-env-flags
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler set-env-flags)))

(defn- sync
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          force    (nth args 1)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (i/sync kv-store force)
        (write-message skey {:type :command-complete})))))

(defn- txlog-watermarks
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (lmdb server skey db-name writing?)]
      (write-message skey
                     {:type :command-complete
                      :result (kv/txlog-watermarks kv-store)}))))

(defn- ha-watermark
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name          (nth args 0)
          kv-store         (lmdb server skey db-name writing?)
          txlog-watermarks (kv/txlog-watermarks kv-store)
          db-state         (get (.-dbs server) db-name)
          authority        (:ha-authority db-state)
          authority-diag   (when authority
                             (try
                               (ctrl/authority-diagnostics authority)
                               (catch Throwable _
                                 nil)))
          txlog-lsn        (long (or (:last-applied-lsn txlog-watermarks) 0))
          runtime-lsn      (when authority
                             (long (dha/read-ha-local-last-applied-lsn
                                    db-state)))
          effective-lsn    (long (or runtime-lsn txlog-lsn))]
      (write-message
       skey
       {:type :command-complete
        :result
        (cond-> {:last-applied-lsn effective-lsn
                 :txlog-last-applied-lsn txlog-lsn
                 :ha-runtime? (boolean authority)
                 :udf-ready? (:udf-ready? db-state)
                 :udf-missing (:udf-missing db-state)
                 :udf-readiness-token (:udf-readiness-token db-state)
                 :ha-authority-owner-node-id
                 (:ha-authority-owner-node-id db-state)
                 :ha-authority-term (:ha-authority-term db-state)
                 :ha-follower-next-lsn (:ha-follower-next-lsn db-state)
                 :ha-follower-last-batch-size
                 (:ha-follower-last-batch-size db-state)
                 :ha-follower-last-sync-ms (:ha-follower-last-sync-ms db-state)
                 :ha-follower-leader-endpoint
                 (:ha-follower-leader-endpoint db-state)
                 :ha-follower-source-endpoint
                 (:ha-follower-source-endpoint db-state)
                 :ha-follower-source-order (:ha-follower-source-order db-state)
                 :ha-follower-last-bootstrap-ms
                 (:ha-follower-last-bootstrap-ms db-state)
                 :ha-follower-bootstrap-source-endpoint
                 (:ha-follower-bootstrap-source-endpoint db-state)
                 :ha-follower-bootstrap-snapshot-last-applied-lsn
                 (:ha-follower-bootstrap-snapshot-last-applied-lsn db-state)
                 :ha-follower-degraded? (:ha-follower-degraded? db-state)
                 :ha-follower-degraded-reason
                 (:ha-follower-degraded-reason db-state)
                 :ha-follower-last-error (:ha-follower-last-error db-state)
                 :ha-follower-last-error-details
                 (:ha-follower-last-error-details db-state)
                 :ha-follower-next-sync-not-before-ms
                 (:ha-follower-next-sync-not-before-ms db-state)
                 :ha-clock-skew-paused? (:ha-clock-skew-paused? db-state)
                 :ha-clock-skew-last-observed-ms
                 (:ha-clock-skew-last-observed-ms db-state)
                 :ha-clock-skew-last-result
                 (:ha-clock-skew-last-result db-state)
                 :ha-lease-until-ms (:ha-lease-until-ms db-state)
                 :ha-last-authority-refresh-ms
                 (:ha-last-authority-refresh-ms db-state)
                 :ha-authority-read-ok? (:ha-authority-read-ok? db-state)
                 :ha-promotion-last-failure
                 (:ha-promotion-last-failure db-state)
                 :ha-promotion-failure-details
                 (:ha-promotion-failure-details db-state)
                 :ha-rejoin-promotion-blocked?
                 (:ha-rejoin-promotion-blocked? db-state)
                 :ha-rejoin-promotion-blocked-until-ms
                 (:ha-rejoin-promotion-blocked-until-ms db-state)
                 :ha-rejoin-promotion-cleared-ms
                 (:ha-rejoin-promotion-cleared-ms db-state)
                 :ha-candidate-since-ms (:ha-candidate-since-ms db-state)
                 :ha-candidate-delay-ms (:ha-candidate-delay-ms db-state)
                 :ha-candidate-pre-cas-wait-until-ms
                 (:ha-candidate-pre-cas-wait-until-ms db-state)
                 :ha-promotion-wait-before-cas-ms
                 (:ha-promotion-wait-before-cas-ms db-state)}
          (some? runtime-lsn)
          (assoc :ha-local-last-applied-lsn runtime-lsn
                 :ha-role (:ha-role db-state))

          authority-diag
          (assoc :ha-control-node-leader? (:node-leader? authority-diag)
                 :ha-control-node-state
                 (some-> (:node-state authority-diag) str)))}))))

(defn- open-tx-log
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          from-lsn (nth args 1)
          upto-lsn (nth args 2 nil)
          kv-store (lmdb server skey db-name writing?)]
      (write-message skey
                     {:type :command-complete
                      :result (kv/open-tx-log kv-store from-lsn upto-lsn)}))))

(defn- open-tx-log-rows
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          from-lsn (nth args 1)
          upto-lsn (nth args 2 nil)
          kv-store (lmdb server skey db-name writing?)]
      (write-message skey
                     {:type :command-complete
                      :result (kv/open-tx-log-rows kv-store
                                                   from-lsn
                                                   upto-lsn)}))))

(defn- read-commit-marker
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (lmdb server skey db-name writing?)]
      (write-message skey
                     {:type :command-complete
                      :result (kv/read-commit-marker kv-store)}))))

(defn- verify-commit-marker!
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (lmdb server skey db-name writing?)]
      (write-message skey
                     {:type :command-complete
                      :result (kv/verify-commit-marker! kv-store)}))))

(defn- force-txlog-sync!
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (write-message skey
                       {:type :command-complete
                        :result (kv/force-txlog-sync! kv-store)})))))

(defn- force-lmdb-sync!
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (write-message skey
                       {:type :command-complete
                        :result (kv/force-lmdb-sync! kv-store)})))))

(defn- create-snapshot!
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (write-message skey
                       {:type :command-complete
                        :result (kv/create-snapshot! kv-store)})))))

(defn- list-snapshots
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (lmdb server skey db-name writing?)]
      (write-message skey
                     {:type :command-complete
                      :result (kv/list-snapshots kv-store)}))))

(defn- snapshot-scheduler-state
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (lmdb server skey db-name writing?)]
      (write-message skey
                     {:type :command-complete
                      :result (kv/snapshot-scheduler-state kv-store)}))))

(defn- txlog-retention-state
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (lmdb server skey db-name writing?)]
      (write-message skey
                     {:type :command-complete
                      :result (kv/txlog-retention-state kv-store)}))))

(defn- gc-txlog-segments!
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name          (nth args 0)
          retain-floor-lsn (nth args 1 nil)
          kv-store         (get-kv-store server db-name)
          sys-conn         (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (write-message skey
                       {:type :command-complete
                        :result (kv/gc-txlog-segments!
                                 kv-store retain-floor-lsn)})))))

(defn- txlog-update-snapshot-floor!
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name               (nth args 0)
          snapshot-lsn          (nth args 1)
          previous-snapshot-lsn (nth args 2 nil)
          kv-store              (get-kv-store server db-name)
          sys-conn              (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (write-message skey
                       {:type :command-complete
                        :result (kv/txlog-update-snapshot-floor!
                                 kv-store snapshot-lsn
                                 previous-snapshot-lsn)})))))

(defn- txlog-clear-snapshot-floor!
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (write-message skey
                       {:type :command-complete
                        :result (kv/txlog-clear-snapshot-floor! kv-store)})))))

(defn- txlog-update-replica-floor!
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name     (nth args 0)
          replica-id  (nth args 1)
          applied-lsn (nth args 2)
          kv-store    (get-kv-store server db-name)
          sys-conn    (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (write-message skey
                       {:type :command-complete
                        :result (kv/txlog-update-replica-floor!
                                 kv-store replica-id applied-lsn)})))))

(defn- txlog-clear-replica-floor!
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name    (nth args 0)
          replica-id (nth args 1)
          kv-store   (get-kv-store server db-name)
          sys-conn   (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (write-message skey
                       {:type :command-complete
                        :result (kv/txlog-clear-replica-floor!
                                 kv-store replica-id)})))))

(defn- txlog-pin-backup-floor!
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name    (nth args 0)
          pin-id     (nth args 1)
          floor-lsn  (nth args 2)
          expires-ms (nth args 3 nil)
          kv-store   (get-kv-store server db-name)
          sys-conn   (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (write-message skey
                       {:type :command-complete
                        :result (kv/txlog-pin-backup-floor!
                                 kv-store pin-id floor-lsn expires-ms)})))))

(defn- txlog-unpin-backup-floor!
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          pin-id   (nth args 1)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (write-message skey
                       {:type :command-complete
                        :result (kv/txlog-unpin-backup-floor!
                                 kv-store pin-id)})))))

(defn- transact-kv
  [^Server server ^SelectionKey skey {:keys [mode args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-store server db-name)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (case mode
          :copy-in (let [txs (copy-in server skey)]
                     ;; copy-in payload format used to include txs in args.
                     ;; Keep backward compatibility while honoring pulled-out
                     ;; dbi-name/key-type/value-type args for compact tx forms.
                     (case (count args)
                       1 (i/transact-kv kv-store txs)
                       2 (i/transact-kv kv-store (nth args 1) txs)
                       3 (i/transact-kv kv-store (nth args 1) txs
                                        (nth args 2))
                       4 (i/transact-kv kv-store (nth args 1) txs
                                        (nth args 2) (nth args 3))
                       5 (i/transact-kv kv-store (nth args 1) txs
                                        (nth args 3) (nth args 4))
                       (u/raise "Invalid :args for transact-kv copy-in"
                                {:args args :mode mode}))
                       (write-message skey {:type :command-complete}))
          :request (normal-kv-store-handler transact-kv)
          (u/raise "Missing :mode when transacting kv" {}))))))

(defn- get-value
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler get-value)))

(defn- get-rank
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler get-rank)))

(defn- get-by-rank
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler get-by-rank)))

(defn- sample-kv
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler sample-kv)))

(defn- get-first
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler get-first)))

(defn- get-first-n
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler get-first-n)))

(defn- run-batch-kv-call
  [kv-store call]
  (let [[op & op-args] call]
    (case op
      :get-value            (apply i/get-value kv-store op-args)
      :get-rank             (apply i/get-rank kv-store op-args)
      :get-by-rank          (apply i/get-by-rank kv-store op-args)
      :sample-kv            (apply i/sample-kv kv-store op-args)
      :get-first            (apply i/get-first kv-store op-args)
      :get-first-n          (apply i/get-first-n kv-store op-args)
      :get-range            (apply i/get-range kv-store op-args)
      :key-range            (apply i/key-range kv-store op-args)
      :key-range-count      (apply i/key-range-count kv-store op-args)
      :key-range-list-count (apply i/key-range-list-count kv-store op-args)
      :range-count          (apply i/range-count kv-store op-args)
      :get-list             (apply i/get-list kv-store op-args)
      :list-count           (apply i/list-count kv-store op-args)
      :in-count?            (apply i/in-list? kv-store op-args)
      :in-list?             (apply i/in-list? kv-store op-args)
      :list-range           (apply i/list-range kv-store op-args)
      :list-range-count     (apply i/list-range-count kv-store op-args)
      :list-range-first     (apply i/list-range-first kv-store op-args)
      :list-range-first-n   (apply i/list-range-first-n kv-store op-args)
      (u/raise "Unsupported batch-kv call"
               {:call op :call-args op-args}))))

(defn- batch-kv
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [[db-name calls] args
          kv-store        (lmdb server skey db-name writing?)]
      (when-not (sequential? calls)
        (u/raise "batch-kv calls must be a sequential collection"
                 {:calls calls}))
      (write-message
        skey
        {:type   :command-complete
         :result (mapv
                   (fn [call]
                     (when-not (sequential? call)
                       (u/raise "Each batch-kv call must be a vector [op & args]"
                                {:call call}))
                     (run-batch-kv-call kv-store call))
                   calls)}))))

(defn- get-range
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          data    (apply i/get-range
                         (lmdb server skey db-name writing?)
                         (rest args))]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- key-range
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          data    (apply i/key-range
                         (lmdb server skey db-name writing?)
                         (rest args))]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- key-range-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler key-range-count)))

(defn- key-range-list-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler key-range-list-count)))

(defn- visit-key-range
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler visit-key-range))))

(defn- range-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler range-count)))

(defn- get-some
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler get-some))))

(defn- range-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen  (nth args 2)
          args    (replace {frozen (b/deserialize frozen)} args)
          db-name (nth args 0)
          data    (apply i/range-filter
                         (lmdb server skey db-name writing?)
                         (rest args))]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- range-keep
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen  (nth args 2)
          args    (replace {frozen (b/deserialize frozen)} args)
          db-name (nth args 0)
          data    (apply i/range-keep
                         (lmdb server skey db-name writing?)
                         (rest args))]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- range-some
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler range-some))))

(defn- range-filter-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler range-filter-count))))

(defn- visit
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler visit))))

(defn- get-list
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          data    (apply i/get-list
                         (lmdb server skey db-name writing?)
                         (rest args))]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- visit-list
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler visit-list))))

(defn- list-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler list-count)))

(defn- in-list?
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler in-list?)))

(defn- list-range
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          data    (apply i/list-range
                         (lmdb server skey db-name writing?)
                         (rest args))]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- list-range-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler list-range-count)))

(defn- list-range-first
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler list-range-first)))

(defn- list-range-first-n
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler list-range-first-n)))

(defn- list-range-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen  (nth args 2)
          args    (replace {frozen (b/deserialize frozen)} args)
          db-name (nth args 0)
          data    (apply i/list-range-filter
                         (lmdb server skey db-name writing?)
                         (rest args))]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- list-range-keep
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen  (nth args 2)
          args    (replace {frozen (b/deserialize frozen)} args)
          db-name (nth args 0)
          data    (apply i/list-range-keep
                         (lmdb server skey db-name writing?)
                         (rest args))]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- list-range-some
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler list-range-some))))

(defn- list-range-filter-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler list-range-filter-count))))

(defn- visit-list-range
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler visit-list-range))))

(defn- q
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [[db-name query inputs] args
          db                     (get-db server db-name writing?)
          inputs                 (replace {:remote-db-placeholder db} inputs)
          data                   (apply q/q query inputs)]
      (if (coll? data)
        (if (< (count data) ^long c/+wire-datom-batch-size+)
          (write-message skey {:type :command-complete :result data})
          (copy-out skey data c/+wire-datom-batch-size+))
        (write-message skey {:type :command-complete :result data})))))

(defn- pull
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [[db-name pattern id opts] args
          db                        (get-db server db-name writing?)
          data                      (d/pull db pattern id opts)]
      (if (coll? data)
        (if (< (count data) ^long c/+wire-datom-batch-size+)
          (write-message skey {:type :command-complete :result data})
          (copy-out skey data c/+wire-datom-batch-size+))
        (write-message skey {:type :command-complete :result data})))))

(defn- pull-many
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [[db-name pattern id opts] args
          db                        (get-db server db-name writing?)
          data                      (d/pull-many db pattern id opts)]
      (if (coll? data)
        (if (< (count data) ^long c/+wire-datom-batch-size+)
          (write-message skey {:type :command-complete :result data})
          (copy-out skey data c/+wire-datom-batch-size+))
        (write-message skey {:type :command-complete :result data})))))

(defn- explain
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [[db-name opts query inputs] args

          db     (get-db server db-name writing?)
          inputs (replace {:remote-db-placeholder db} inputs)
          data   (apply q/explain opts query inputs)]
      (write-message skey {:type :command-complete :result data}))))

(defn- fulltext-datoms
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [[db-name query opts] args
          db                   (get-db server db-name writing?)
          data                 (dbq/fulltext-datoms db query opts)]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- add-doc
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler add-doc)))

(defn- remove-doc
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler remove-doc)))

(defn- clear-docs
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler clear-docs)))

(defn- doc-indexed?
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler doc-indexed?)))

(defn- doc-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler doc-count)))

(defn- search
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler search)))

(defn- search-re-index
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[db-name opts] args
          engine         (i/re-index (search-engine server skey db-name) opts)]
      (update-db server db-name #(assoc % :engine engine))
      (write-message skey {:type :command-complete}))))

(defn- add-vec
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (vector-handler add-vec)))

(defn- remove-vec
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (vector-handler remove-vec)))

(defn- persist-vecs
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (vector-handler persist-vecs)))

(defn- close-vecs
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (vector-handler close-vecs)))

(defn- clear-vecs
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (vector-handler clear-vecs)))

(defn- vec-indexed?
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (vector-handler vec-indexed?)))

(defn- vecs-info
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (vector-handler vecs-info)))

(defn- search-vec
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (vector-handler search-vec)))

(defn- vec-re-index
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[db-name opts] args
          old            (vector-index server skey db-name)
          new            (i/re-index old opts)]
      (update-db server db-name #(assoc % :index new))
      (write-message skey {:type :command-complete}))))

(defn- kv-re-index
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[db-name opts] args
          db             (i/re-index (lmdb server skey db-name false) opts)]
      (update-db server db-name #(assoc % :store db))
      (write-message skey {:type :command-complete}))))

(defn- datalog-re-index
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[db-name schema opts] args
          db                    (get-in (.-dbs server) [db-name :dt-db])
          conn                  (atom db)
          conn1                 (dump/re-index-datalog conn schema opts)
          ^DB db1               @conn1
          store1                (.-store db1)]
      (update-db server db-name #(assoc % :store store1 :dt-db db1))
      (write-message skey {:type :command-complete}))))

;; END message handlers

(defn- current-ha-txlog-term
  [^Server server db-name]
  (when-let [db-state (and db-name (get (.-dbs server) db-name))]
    (let [authority-term (:ha-authority-term db-state)]
      (when (and (:ha-authority db-state)
                 (= :leader (:ha-role db-state))
                 (integer? authority-term)
                 (pos? ^long authority-term))
        (long authority-term)))))

(defn- dispatch-message-with-ha-write-admission
  [^Server server ^SelectionKey skey message]
  (let [type (:type message)
        db-name (nth (:args message) 0 nil)
        ha-txlog-term (current-ha-txlog-term server db-name)
        precheck-only? (contains? #{:open-transact :open-transact-kv} type)
        {:keys [ok? error]}
        (with-ha-write-admission
          server
          message
          #(when-not precheck-only?
             (binding [txlog/*commit-payload-ha-term* ha-txlog-term
                       cpp/*before-write-commit-fn*
                       (ha-write-commit-check-fn server message)]
               (message-cases skey type))))]
    (cond
      (not ok?)
      (error-response skey "HA write admission rejected" error)

      precheck-only?
      (message-cases skey type))))

(defprotocol IRunner
  "Ensure calls within `with-transaction-kv` run in the same thread that
  runs `open-transact-kv`, otherwise LMDB will deadlock"
  (new-message [this skey message])
  (run-calls [this])
  (halt-run [this]))

(deftype Runner [server kv-store sk msg running?]
  IRunner
  (new-message [_ skey message]
    (vreset! sk skey)
    (vreset! msg message))

  (halt-run [_] (vreset! running? false))

  (run-calls [_]
    (locking kv-store
      (loop []
        (.wait ^Object kv-store)
        (let [message @msg
              skey    @sk]
          (dispatch-message-with-ha-write-admission server skey message))
        (when @running? (recur))))))

(defn- write-txn-runner
  [^Server server db-name kv-store]
  (let [runner (->Runner server kv-store (volatile! nil)
                         (volatile! nil) (volatile! true))]
    (update-db server db-name #(assoc % :runner runner))
    runner))

(defn- execute
  "Execute a function in a thread from the worker thread pool"
  [^Server server f]
  (.execute ^Executor (.-work-executor server) f))

(def ^:private idempotent-withtxn-control-types
  #{:close-transact
    :abort-transact
    :close-transact-kv
    :abort-transact-kv})

(defn- handle-writing
  [^Server server ^SelectionKey skey {:keys [args] :as message}]
  (try
    (let [db-name  (nth args 0)
          type     (:type message)
          kv-store (get-kv-store server db-name)
          runner   (get-in (.-dbs server) [db-name :runner])]
      (cond
        runner
        (do
          (new-message runner skey message)
          (locking kv-store (.notify kv-store)))

        (idempotent-withtxn-control-types type)
        (write-message skey {:type :command-complete})

        :else
        (u/raise "No active with-transaction runner"
                 {:db-name db-name
                  :type    type})))
    (catch Exception e
      ;; (stt/print-stack-trace e)
      (error-response skey (str "Error Handling with-transaction message:"
                                (ex-message e)) {}))))

(defn- set-last-active
  [^Server server ^SelectionKey skey]
  (let [{:keys [client-id]} @(.attachment skey)]
    (when client-id
      ;; Avoid durable session writes on every request; this path is hot.
      (when-let [session (get-client server client-id)]
        (.put ^Map (.-clients server) client-id
              (assoc session :last-active (System/currentTimeMillis)))))))

(defn- handle-message
  [^Server server ^SelectionKey skey fmt msg ]
  (try
    (let [wire-opts                       (:wire-opts @(.attachment skey))
          {:keys [writing?] :as message}
          (p/read-value fmt msg wire-opts)]
      (log/debug "Message received:" (dissoc message :password :args))
      (set-last-active server skey)
      (if writing?
        (handle-writing server skey message)
        (with-db-runtime-read-access
          server
          message
          #(dispatch-message-with-ha-write-admission server skey message))))
    (catch Exception e
      ;; (stt/print-stack-trace e)
      (log/error "Error Handling message:" e))))

(defn- handle-read
  [^Server server ^SelectionKey skey]
  (try
    (let [state                         (.attachment skey)
          {:keys [^ByteBuffer read-bf]} @state
          capacity                      (.capacity read-bf)
          ^SocketChannel ch             (.channel skey)
          ^int readn                    (p/read-ch ch read-bf)]
      (cond
        (> readn 0)  (if (= (.position read-bf) capacity)
                       (let [size (* ^long c/+buffer-grow-factor+ capacity)
                             bf   (bf/allocate-buffer size)]
                         (.flip read-bf)
                         (bf/buffer-transfer read-bf bf)
                         (vswap! state assoc :read-bf bf))
                       (p/extract-message
                         read-bf
                         (fn [fmt msg]
                           (execute
                             server #(handle-message server skey fmt msg)))))
        (= readn 0)  :continue
        (= readn -1) (.close ch)))
    (catch java.io.IOException e
      (if (s/includes? (ex-message e) "Connection reset by peer")
        (.close (.channel skey))
        (log/error "Read IOException:" (ex-message e))))
    (catch Exception e
      ;; (stt/print-stack-trace e)
      (log/error "Read error:" (ex-message e)))))

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
  (let [timeout (.-idle-timeout server)
        clients (.-clients server)]
    (doseq [[client-id session] clients
            :let                [{:keys [last-active]} session]]
      (if last-active
        (when (< timeout (- (System/currentTimeMillis) ^long last-active))
          (disconnect-client* server client-id))
        ;; migrate old sessions that don't have last-active
        (update-client server client-id
                       #(assoc % :last-active (System/currentTimeMillis)))))))

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
