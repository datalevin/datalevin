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
   [datalevin.server.handlers :as sh]
   [datalevin.server.ha-runtime :as hrt]
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

(declare event-loop close-conn store->db-name session-lmdb remove-store
         halt-run)

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
  (when client-id
    (get (.-clients server) client-id)))

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
(def ^:private same-ha-runtime-context? hrt/same-ha-runtime-context?)
(def ^:private same-ha-runtime-state? hrt/same-ha-runtime-state?)
(def ^:private merge-ha-follower-local-side-effect-patch
  hrt/merge-ha-follower-local-side-effect-patch)
(def ^:private merge-ha-follower-side-effect-patch
  hrt/merge-ha-follower-side-effect-patch)
(def ^:private merge-ha-renew-state-patch
  hrt/merge-ha-renew-state-patch)
(def ^:private merge-ha-renew-promotion-state-patch
  hrt/merge-ha-renew-promotion-state-patch)
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

(defn- fresh-runtime-db-info
  [store]
  (when (instance? Store store)
    (let [lmdb          (kv/raw-lmdb (.-lmdb ^Store store))
          max-tx        (long (or (i/get-value lmdb c/meta :max-tx :attr :long)
                                  (i/max-tx store)))
          last-modified (long (or (i/get-value lmdb c/meta :last-modified
                                               :attr :long)
                                  (i/last-modified store)))]
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

(declare get-lock
         db-write-admission-lock
         with-db-runtime-store-read-access
         with-db-runtime-store-swap)

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
            (drep/apply-ha-follower-txlog-record! expected-state record)
            (u/raise "HA follower replay aborted because follower state changed"
                     {:error :ha/follower-stale-state
                      :db-name db-name
                      :record-lsn (:lsn record)
                      :state current-state
                      :current-role (:ha-role current-state)
                      :expected-role (:ha-role expected-state)}))))
      (finally
        (.release lock)))))

(defn- with-ha-follower-replay-quiesced
  [^Server server db-name f]
  (let [^Semaphore lock (get-lock server db-name)]
    (.acquire lock)
    (try
      (locking (db-write-admission-lock server db-name)
        (f))
      (finally
        (.release lock)))))

(def ^:private ha-loop-sleep-ms hrt/ha-loop-sleep-ms)
(def ^:private ha-follower-loop-sleep-ms hrt/ha-follower-loop-sleep-ms)
(def ^:private sleep-ha-loop! hrt/sleep-ha-loop!)
(def ^:private ha-loop-error-backoff! hrt/ha-loop-error-backoff!)

(defn- ha-renew-promotion-result?
  [expected-state next-state]
  (and (not (contains? #{:leader :demoting} (:ha-role expected-state)))
       (contains? #{:leader :demoting} (:ha-role next-state))
       (= (:ha-node-id next-state)
          (:ha-authority-owner-node-id next-state))))

(defn- publish-ha-renew-state!
  [^Server server db-name expected-state next-state ^AtomicBoolean running?]
  (let [renew-patch (ha-renew-state-patch expected-state next-state)
        promotion-result? (ha-renew-promotion-result? expected-state next-state)
        publish!
        (fn []
          (let [{:keys [updated? state]}
                (replace-db-state-if-current
                 server
                 db-name
                 expected-state
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
                      expected-state
                      renew-patch)))
                  state)
                state
                (if (and promotion-result?
                         renew-patch
                         (or (nil? state)
                             (not (contains? #{:leader :demoting}
                                             (:ha-role state)))))
                  (:state
                   (transform-db-state-when
                    server
                    db-name
                    #(same-ha-runtime-state? %
                                            expected-state
                                            :ha-renew-loop-running?)
                    #(merge-ha-renew-promotion-state-patch
                      %
                      expected-state
                      renew-patch)))
                  state)]
            (when (and promotion-result?
                       (or (nil? state)
                           (not (contains? #{:leader :demoting}
                                           (:ha-role state)))))
              (log/warn "HA renew promotion could not publish local leader state"
                        {:db-name db-name
                         :expected-role (:ha-role expected-state)
                         :next-role (:ha-role next-state)
                         :state-role (:ha-role state)}))
            state))]
    (if (and promotion-result?
             (= :follower (:ha-role expected-state)))
      ;; Followers and leaders share the same underlying store handle. Serialize
      ;; follower replay and follower->leader publication so replay cannot keep
      ;; mutating the store after local promotion publishes.
      (with-ha-follower-replay-quiesced server db-name publish!)
      (publish!))))

(declare log-ha-loop-crash!)

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
              (let [next-state (binding [drep/*ha-current-state-fn*
                                         #(get (.-dbs server) db-name)
                                         drep/*ha-with-local-store-read-fn*
                                         (fn [f]
                                           (with-db-runtime-store-read-access
                                             server
                                             db-name
                                             f))]
                                 (ha-renew-step db-name m))
                    state (publish-ha-renew-state!
                           server
                           db-name
                           m
                           next-state
                           running?)]
                (if (or (nil? state)
                        (nil? (:ha-authority state))
                        (not (identical? running?
                                         (:ha-renew-loop-running? state))))
                  (.set running? false)
                  (sleep-ha-loop! running? (ha-loop-sleep-ms state))))))
          (catch Throwable t
            (log-ha-loop-crash!
             "HA renew loop crashed; retrying after backoff"
             db-name
             t)
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
              (let [next-state (binding [drep/*ha-current-state-fn*
                                         #(get (.-dbs server) db-name)
                                         drep/*ha-follower-apply-record-fn*
                                         (fn [state record]
                                           (ha-follower-apply-record-with-guard
                                            server
                                            db-name
                                            state
                                            record))
                                         drep/*ha-with-local-store-swap-fn*
                                         (fn [f]
                                           (with-db-runtime-store-swap
                                             server
                                             db-name
                                             f))
                                         drep/*ha-with-local-store-read-fn*
                                         (fn [f]
                                           (with-db-runtime-store-read-access
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
            (log-ha-loop-crash!
             "HA follower sync loop crashed; retrying after backoff"
             db-name
             t)
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
    (or (and write?
             db-name
             (not (contains? udf-admission-exempt-write-types
                             (:type message)))
             (udf-write-admission-error db-name m))
        (dha/ha-write-admission-error (.-dbs server) message))))

(defn- leader-authority-state?
  [m]
  (and (= :leader (:ha-role m))
       (satisfies? ctrl/ILeaseAuthority (:ha-authority m))))

(defn- refresh-ha-write-commit-state!
  [^Server server db-name]
  (let [m (get (.-dbs server) db-name)]
    (if-not (leader-authority-state? m)
      m
      (let [timeout-ms (hu/ha-request-timeout-ms
                        m
                        (or (get-in m [:ha-control-plane :operation-timeout-ms])
                            5000))
            next-state (dha/refresh-ha-authority-state db-name m timeout-ms)
            refresh-patch (hrt/ha-authority-refresh-state-patch
                           m
                           next-state)
            {:keys [updated? state]}
            (replace-db-state-if-current
             server
             db-name
             m
             leader-authority-state?
             next-state)]
        (if (or updated?
                (nil? refresh-patch))
          state
          (:state
           (transform-db-state-when
            server
            db-name
            #(and (leader-authority-state? %)
                  (hrt/same-ha-runtime-state?
                   %
                   m
                   :ha-renew-loop-running?))
            #(hrt/merge-ha-authority-refresh-state-patch
              %
              m
              refresh-patch))))))))

(defn- ha-write-commit-admission!
  [^Server server message]
  (let [db-name (nth (:args message) 0 nil)]
    (when db-name
      (refresh-ha-write-commit-state! server db-name)))
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
      ;; This request-time gate is a cached-state fast path only. The
      ;; authoritative HA check runs again at commit, so one-shot writes do
      ;; not need to serialize through the per-DB admission lock here.
      (if-let [err (ha-write-admission-error server message)]
        {:ok? false
         :error err}
        {:ok? true
         :result (f)})
      {:ok? true
       :result (f)})))

(def ^:private ha-abort-cleanup-types
  #{:abort-transact
    :abort-transact-kv})

(def ^:private ha-rejected-close-cleanup-types
  #{:close-transact
    :close-transact-kv})

(defn- cleanup-rejected-close-transact!
  [^Server server {:keys [type args]}]
  (let [db-name (nth args 0 nil)
        dbs     (.-dbs server)
        runner  (and db-name (get-in dbs [db-name :runner]))]
    (when (and db-name runner (ha-rejected-close-cleanup-types type))
      (let [kv-store (get-kv-store server db-name)
            ^Semaphore lock (get-in dbs [db-name :lock])]
        (try
          (i/abort-transact-kv kv-store)
          (i/close-transact-kv kv-store)
          (catch Throwable t
            (let [details (cond-> {:db-name db-name
                                   :type type
                                   :error-class (.getName ^Class (class t))}
                            (some? (ex-message t))
                            (assoc :message (ex-message t)))]
              (log/warn
                "Failed to clean up rejected HA close-transact"
                details)
              (log/debug t
                         "Rejected HA close-transact cleanup stack trace"
                         {:db-name db-name
                          :type type})))
          (finally
            (halt-run runner)
            (update-db server db-name
                       #(dissoc % :runner :wlmdb :wstore :wdt-db))
            (when lock
              (.release lock))))))))

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
                             ;; Preserve client message order per connection.
                             ;; Authentication/session setup must not race
                             ;; with subsequent requests like :open.
                             :message-lock (Object.)
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
            (long (drep/read-ha-snapshot-payload-lsn {:store store}))
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
        (log-handled-request-error! e)
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
        cleanup-only? (ha-abort-cleanup-types type)
        write? (and (not cleanup-only?) (dha/ha-write-message? message))
        db-name (nth (:args message) 0 nil)
        ha-txlog-term (current-ha-txlog-term server db-name)
        precheck-only? (contains? #{:open-transact :open-transact-kv} type)
        {:keys [ok? error]}
        (if cleanup-only?
          {:ok? true}
          (with-ha-write-admission
            server
            message
            #(cond
               precheck-only?
               nil

               write?
               (binding [txlog/*commit-payload-ha-term* ha-txlog-term
                         cpp/*before-write-commit-fn*
                         (ha-write-commit-check-fn server message)]
                 (dispatch-message server skey message))

               :else
               (dispatch-message server skey message))))]
    (cond
      (not ok?)
      (do
        (cleanup-rejected-close-transact! server message)
        (error-response skey "HA write admission rejected" error))

      cleanup-only?
      (dispatch-message server skey message)

      precheck-only?
      (dispatch-message server skey message))))

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
  (if-let [handler (get message-handler-map (:type message))]
    (handler server skey message)
    (error-response skey
                    (str "Unknown message type " (:type message))
                    {})))

(defn- execute
  "Execute a function in a thread from the worker thread pool"
  [^Server server f]
  (.execute ^Executor (.-work-executor server) f))

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
  (try
    (let [db-name  (nth args 0)
          type     (:type message)
          kv-store (get-kv-store server db-name)
          runner   (get-in (.-dbs server) [db-name :runner])]
      (cond
        runner
        (do
          (trace-remote-tx! "handle-writing" type db-name)
          (new-message runner skey message))

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
    (let [state (.attachment skey)
          {:keys [message-lock]} @state
          message
          (locking message-lock
            (let [wire-opts (:wire-opts @state)
                  {:keys [type] :as message}
                  (p/read-value fmt msg wire-opts)]
              (if (= type :set-client-id)
                (do
                  (log/debug "Message received:" (dissoc message :password :args))
                  (dispatch-message server skey message)
                  ::handled)
                message)))]
      (when-not (= ::handled message)
        (let [{:keys [writing?]} message]
          (log/debug "Message received:" (dissoc message :password :args))
          (set-last-active server skey)
          (if writing?
            (handle-writing server skey message)
            (with-db-runtime-read-access
              server
              message
              #(dispatch-message-with-ha-write-admission server skey message))))))
    (catch Exception e
      ;; (stt/print-stack-trace e)
      (log/error "Error Handling message:" e))))

(defn- handle-read
  [^Server server ^SelectionKey skey]
  (try
    (let [state                         (.attachment skey)
          {:keys [^ByteBuffer read-bf
                  message-lock]}        @state
          capacity                      (.capacity read-bf)
          ^SocketChannel ch             (.channel skey)
          ^int readn                    (p/read-ch ch read-bf)]
      (when (pos? readn)
        (trace-remote-tx! "handle-read" readn (.hashCode skey)))
      (when (> (.position read-bf) c/message-header-size)
        (let [^ByteBuffer probe (.duplicate read-bf)
              pos (.position probe)]
          (.flip probe)
          (trace-remote-tx! "buffer-header"
                            (.hashCode skey)
                            "pos" pos
                            "len" (.getInt (doto probe (.get))))))
      (cond
        (> readn 0)  (do
                       (p/extract-message
                         read-bf
                         (fn [fmt msg]
                           (execute server #(handle-message server skey fmt msg))))
                       (when (= (.position read-bf) capacity)
                         (let [size (* ^long c/+buffer-grow-factor+ capacity)
                               bf   (bf/allocate-buffer size)]
                           (.flip read-bf)
                           (bf/buffer-transfer read-bf bf)
                           (vswap! state assoc :read-bf bf))))
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
