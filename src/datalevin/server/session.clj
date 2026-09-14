;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server.session
  "Client tracking and session bookkeeping."
  (:require
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.lmdb :as l]
   [datalevin.server.resources :as resources]
   [taoensso.timbre :as log])
  (:import
   [java.nio.channels SelectionKey]
   [java.util Map UUID]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.function BiFunction]))

(def session-dbi "datalevin-server/sessions")

(def session-deps-contract
  "Callbacks `datalevin.server` must inject for session bookkeeping."
  {:callbacks
   #{:cleanup-connection-transactions-fn :clients-fn :close-conn-fn
     :close-store-fn :consensus-ha-opts-fn :current-runtime-opts-fn
     :ensure-ha-runtime-fn :get-ip-fn :idle-timeout-fn :new-runtime-db-fn
     :now-ms-fn :open-store-fn :password-matches?-fn :perm-tgt-name-fn
     :pull-user-fn :resolved-runtime-opts-fn :connection-keys-fn :sys-conn-fn
     :user-eid-fn :user-permissions-fn :user-roles-fn}})

(defn session-lmdb
  [sys-conn]
  (let [db ^datalevin.db.DB (d/db sys-conn)]
    (.-lmdb ^datalevin.storage.Store (.-store db))))

(defn get-client
  [clients client-id]
  (when client-id
    (get clients client-id)))

(defn- activity-checkpoint-ms
  ^long [deps server]
  (max 1 (min 60000 (quot (long ((:idle-timeout-fn deps) server)) 4))))

(defn- persist-session!
  [deps server client-id session]
  (let [session (assoc session :last-active-checkpoint-until
                       (+ (long (:last-active session))
                          (activity-checkpoint-ms deps server)))]
    (d/transact-kv
      (session-lmdb ((:sys-conn-fn deps) server))
      [(l/kv-tx :put session-dbi client-id (with-meta session nil) :uuid :data)])
    session))

(defn touch-client
  "Record activity, checkpointing at most once per minute (or a quarter of
  the idle timeout). Persist before accepting activity beyond the saved window."
  [deps server client-id]
  (when client-id
    (.computeIfPresent
      ^ConcurrentHashMap ((:clients-fn deps) server) client-id
      (reify BiFunction
        (apply [_ _ current]
          (let [now-ms (long ((:now-ms-fn deps)))
                session (-> current
                            (assoc :last-active now-ms)
                            (vary-meta dissoc ::restored?))]
            (if (>= now-ms (long (or (:last-active-checkpoint-until current) 0)))
              (persist-session! deps server client-id session)
              session)))))))

(defn add-client
  [deps server ip client-id username]
  (let [sys-conn ((:sys-conn-fn deps) server)
        roles    ((:user-roles-fn deps) sys-conn username)
        perms    ((:user-permissions-fn deps) sys-conn username)
        session  {:ip          ip
                  :uid         ((:user-eid-fn deps) sys-conn username)
                  :username    username
                  :last-active ((:now-ms-fn deps))
                  :stores      {}
                  :engines     #{}
                  :indices     #{}
                  :dt-dbs      #{}
                  :roles       roles
                  :permissions perms}
        clients  ((:clients-fn deps) server)]
    ;; All session writers use the map's per-key computation lock, including
    ;; persistence. A delete must not commit between a read and its later put.
    (.compute ^ConcurrentHashMap clients client-id
              (reify BiFunction
                (apply [_ _ _]
                  (persist-session! deps server client-id session))))
    (log/info "Added client " client-id
              "from:" ip
              "for user:" username)))

(defn remove-client
  "Delete the persisted and live session in order with other session changes.
  A nil client ID is ignored."
  [deps server client-id]
  (when client-id
    (let [clients ((:clients-fn deps) server)]
      (.compute ^ConcurrentHashMap clients client-id
                (reify BiFunction
                  (apply [_ _ _]
                    (d/transact-kv
                      (session-lmdb ((:sys-conn-fn deps) server))
                      [(l/kv-tx :del session-dbi client-id :uuid)])
                    nil)))
      (log/info "Removed client:" client-id))))

(defn update-client
  "Atomically update and persist an existing session. Nil or missing IDs are
  ignored without invoking f. Returns the updated session, or nil if absent."
  [deps server client-id f]
  (when client-id
    (.computeIfPresent
      ^ConcurrentHashMap ((:clients-fn deps) server) client-id
      (reify BiFunction
        (apply [_ _ current]
          (persist-session! deps server client-id (f current)))))))

(defn load-sessions
  "Restore activity windows without renewing them on each restart. Legacy
  records have unbounded timestamp lag, so migrate them once to the current time."
  ([sys-conn] (load-sessions sys-conn (System/currentTimeMillis)))
  ([sys-conn now-ms]
   (let [lmdb (session-lmdb sys-conn)]
     (d/open-dbi lmdb session-dbi)
     (let [sessions (d/get-range lmdb session-dbi [:all] :uuid :data)
           migrated (into {}
                          (keep (fn [[id session]]
                                  (when-not (:last-active-checkpoint-until session)
                                    [id (assoc session :last-active now-ms
                                                       :last-active-checkpoint-until now-ms)])))
                          sessions)]
       (when (seq migrated)
         (d/transact-kv lmdb
                        (mapv (fn [[id session]]
                                (l/kv-tx :put session-dbi id session :uuid :data))
                              migrated)))
       (ConcurrentHashMap.
         ^Map (into {}
                    (map (fn [[id session]]
                           [id (vary-meta (get migrated id session)
                                          assoc ::restored? true)]))
                    sessions))))))

(defn flush-sessions!
  "Save exact activity after request workers stop. An untouched restored
  session keeps its existing crash allowance, without extending it."
  [deps server]
  (let [^ConcurrentHashMap clients ((:clients-fn deps) server)]
    (resources/close-all!
      (for [client-id (keys clients)]
        #(.computeIfPresent
           clients client-id
           (reify BiFunction
             (apply [_ _ current]
               (let [session (if (::restored? (meta current))
                               current
                               (assoc current :last-active-checkpoint-until
                                              (:last-active current)))]
                 (d/transact-kv
                   (session-lmdb ((:sys-conn-fn deps) server))
                   [(l/kv-tx :put session-dbi client-id
                             (with-meta session nil) :uuid :data)])
                 session))))))))

(defn reopen-dbs
  [deps root clients ^ConcurrentHashMap dbs]
  (doseq [[_ {:keys [stores engines indices dt-dbs]}] clients]
    (doseq [[db-name {:keys [datalog? dbis consensus-ha?]}]
            stores
            :when (not (get-in dbs [db-name :store]))
            :let  [m (get dbs db-name {})]]
      (if consensus-ha?
        ;; Consensus HA runtime identity is node-local. Persist the
        ;; classification at explicit open time so restart can skip automatic
        ;; reopen entirely, instead of probing the store and triggering txlog
        ;; recovery before the fresh HA peer open happens.
        (log/info "Skipping automatic reopen of consensus HA database"
                  {:db-name db-name
                   :root root})
        (resources/with-acquired
          (fn [own!]
            (let [dt-db-v (volatile! nil)
                  store (own! ((:open-store-fn deps) root db-name dbis datalog?)
                              #(if-let [dt-db @dt-db-v]
                                 (db/close-db dt-db)
                                 ((:close-store-fn deps) %)))
                  consensus-ha? (and datalog?
                                     (some? ((:consensus-ha-opts-fn deps) store)))]
              (if consensus-ha?
                (do
                  ;; Backward compatibility for persisted sessions created before
                  ;; the stored `:consensus-ha?` flag existed.
                  ((:close-store-fn deps) store)
                  (log/info "Skipping automatic reopen of consensus HA database"
                            {:db-name db-name
                             :root root}))
                (let [runtime-opts ((:resolved-runtime-opts-fn deps) nil db-name store m)
                      next-m       ((:ensure-ha-runtime-fn deps)
                                    root
                                    db-name
                                    (cond-> (assoc m
                                                   :store store
                                                   :runtime-opts runtime-opts)
                                      datalog?
                                      (assoc :dt-db
                                             (vreset! dt-db-v
                                                      ((:new-runtime-db-fn deps)
                                                       store
                                                       runtime-opts))))
                                    store)]
                  (.put dbs db-name next-m))))))))
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
                   ((:new-runtime-db-fn deps)
                    (get-in dbs [db-name :store])
                    ((:current-runtime-opts-fn deps) m)))))))

(defn authenticate
  [deps server skey {:keys [username password]}]
  (when-let [{:keys [user/pw-salt user/pw-hash]}
             ((:pull-user-fn deps) ((:sys-conn-fn deps) server) username)]
    (when ((:password-matches?-fn deps) password pw-hash pw-salt)
      (let [client-id (UUID/randomUUID)
            ip        ((:get-ip-fn deps) skey)]
        (add-client deps server ip client-id username)
        client-id))))

(defn client-display
  [deps server [client-id m]]
  (let [sys-conn ((:sys-conn-fn deps) server)]
    [client-id
     (-> m
         (update :permissions
                 #(mapv
                   (fn [{:keys [permission/act permission/obj
                                permission/tgt]}]
                     (if-let [{:keys [db/id]} tgt]
                       [act obj ((:perm-tgt-name-fn deps) sys-conn obj id)]
                       [act obj]))
                   %))
         (assoc :open-dbs (:stores m))
         (select-keys [:ip :username :roles :permissions :open-dbs]))]))

(defn- close-client-connections!
  [deps server client-id]
  (resources/close-all!
    (for [^SelectionKey k ((:connection-keys-fn deps) server)
          :let [state (.attachment k)]
          :when (and state (= client-id (@state :client-id)))]
      #(try
         ((:cleanup-connection-transactions-fn deps) server k)
         (finally ((:close-conn-fn deps) k))))))

(defn disconnect-client*
  [deps server client-id]
  (close-client-connections! deps server client-id)
  (remove-client deps server client-id))

(defn disconnect-user
  [deps server tgt-username]
  (doseq [[client-id {:keys [username]}] ((:clients-fn deps) server)
          :when                          (= tgt-username username)]
    (disconnect-client* deps server client-id)))

(defn update-cached-role
  [deps server target-username]
  (let [sys-conn    ((:sys-conn-fn deps) server)
        roles       ((:user-roles-fn deps) sys-conn target-username)
        permissions ((:user-permissions-fn deps) sys-conn target-username)]
    (doseq [cid (keep (fn [[client-id {:keys [username]}]]
                        (when (= target-username username) client-id))
                      ((:clients-fn deps) server))]
      (update-client deps server cid
                     #(assoc % :roles roles :permissions permissions)))))

(defn update-cached-permission
  [deps server target-role]
  (let [sys-conn ((:sys-conn-fn deps) server)]
    (doseq [[cid uname] (keep (fn [[client-id {:keys [username roles]}]]
                                (when (some #(= % target-role) roles)
                                  [client-id username]))
                              ((:clients-fn deps) server))]
      (update-client deps server cid
                     #(assoc % :permissions
                             ((:user-permissions-fn deps) sys-conn uname))))))

(defn- idle-session?
  [session ^long now-ms ^long timeout]
  (when-let [last-active (:last-active session)]
    (let [baseline (if (::restored? (meta session))
                     (max (long last-active)
                          (long (or (:last-active-checkpoint-until session)
                                    last-active)))
                     (long last-active))]
      (< timeout (- now-ms baseline)))))

(defn- expire-idle-session!
  [deps server ^ConcurrentHashMap clients client-id now-ms timeout]
  (let [expired? (volatile! false)]
    ;; Recheck under the same per-session lock as touches and persistence.
    ;; A snapshot taken before a request must not delete its fresh activity.
    (.computeIfPresent
      clients client-id
      (reify BiFunction
        (apply [_ _ session]
          (cond
            (nil? (:last-active session))
            (persist-session! deps server client-id
                              (assoc session :last-active now-ms))

            (idle-session? session now-ms timeout)
            (do
              (d/transact-kv
                (session-lmdb ((:sys-conn-fn deps) server))
                [(l/kv-tx :del session-dbi client-id :uuid)])
              (vreset! expired? true)
              nil)

            :else session))))
    (when @expired?
      (close-client-connections! deps server client-id)
      (log/info "Removed idle client:" client-id))))

(defn remove-idle-sessions
  "Sweep all sessions, then report failures so the caller can back off without
  a malformed session or failed delete preventing cleanup of other clients."
  [deps server]
  (let [^long timeout ((:idle-timeout-fn deps) server)
        clients ((:clients-fn deps) server)
        now-ms  (long ((:now-ms-fn deps)))
        failure (volatile! nil)
        failed-count (volatile! 0)]
    (doseq [[client-id snapshot] clients]
      (try
        ;; Do not wait on persistence for active sessions. Keep even this
        ;; timestamp check inside the boundary for malformed legacy records.
        (when (or (nil? (:last-active snapshot))
                  (idle-session? snapshot now-ms timeout))
          (expire-idle-session! deps server clients client-id now-ms timeout))
        (catch InterruptedException e (throw e))
        (catch Exception e
          (when-not @failure (vreset! failure e))
          (vreset! failed-count (inc (long @failed-count))))))
    (when-let [cause @failure]
      (throw (ex-info "Idle session cleanup failed"
                      {:error :server/session-cleanup-failed
                       :failed-count @failed-count}
                      cause)))))
