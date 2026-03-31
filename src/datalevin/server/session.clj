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
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.lmdb :as l]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [java.util Map UUID]
   [java.util.concurrent ConcurrentHashMap]))

(def session-dbi "datalevin-server/sessions")

(defn session-lmdb
  [sys-conn]
  (let [db ^datalevin.db.DB (d/db sys-conn)]
    (.-lmdb ^datalevin.storage.Store (.-store db))))

(defn get-client
  [clients client-id]
  (when client-id
    (get clients client-id)))

(defn add-client
  [deps server ip client-id username]
  (let [sys-conn ((:sys-conn-fn deps) server)
        roles    ((:user-roles-fn deps) sys-conn username)
        perms    ((:user-permissions-fn deps) sys-conn username)
        session  {:ip          ip
                  :uid         ((:user-eid-fn deps) sys-conn username)
                  :username    username
                  :last-active (System/currentTimeMillis)
                  :stores      {}
                  :engines     #{}
                  :indices     #{}
                  :dt-dbs      #{}
                  :roles       roles
                  :permissions perms}
        clients  ((:clients-fn deps) server)]
    (d/transact-kv (session-lmdb sys-conn)
                   [(l/kv-tx :put session-dbi client-id session :uuid :data)])
    (.put ^Map clients client-id session)
    (log/info "Added client " client-id
              "from:" ip
              "for user:" username)))

(defn remove-client
  [deps server client-id]
  (let [sys-conn ((:sys-conn-fn deps) server)
        clients  ((:clients-fn deps) server)]
    (d/transact-kv (session-lmdb sys-conn)
                   [(l/kv-tx :del session-dbi client-id :uuid)])
    (.remove ^Map clients client-id)
    (log/info "Removed client:" client-id)))

(defn update-client
  [deps server client-id f]
  (let [clients  ((:clients-fn deps) server)
        session  (f (get-client clients client-id))
        sys-conn ((:sys-conn-fn deps) server)]
    (d/transact-kv (session-lmdb sys-conn)
                   [(l/kv-tx :put session-dbi client-id session :uuid :data)])
    (.put ^Map clients client-id session)))

(defn load-sessions
  [sys-conn]
  (let [lmdb (session-lmdb sys-conn)]
    (d/open-dbi lmdb session-dbi)
    (ConcurrentHashMap.
     ^Map (into {} (d/get-range lmdb session-dbi [:all] :uuid :data)))))

(defn reopen-dbs
  [deps root clients ^ConcurrentHashMap dbs]
  (doseq [[_ {:keys [stores engines indices dt-dbs]}] clients]
    (doseq [[db-name {:keys [datalog? dbis]}]
            stores
            :when (not (get-in dbs [db-name :store]))
            :let  [m (get dbs db-name {})]]
      (let [store ((:open-store-fn deps) root db-name dbis datalog?)
            consensus-ha? (and datalog?
                               (some? ((:consensus-ha-opts-fn deps) store)))]
        (if consensus-ha?
          (do
            ;; Consensus HA runtime identity is node-local. Restoring a DB from
            ;; persisted client sessions before a fresh explicit open can start
            ;; the wrong peer from stale store metadata after restart.
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
                                       ((:new-runtime-db-fn deps)
                                        store
                                        runtime-opts)))
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

(defn disconnect-client*
  [deps server client-id]
  (remove-client deps server client-id)
  (let [selector ((:selector-fn deps) server)]
    (when (.isOpen selector)
      (doseq [^java.nio.channels.SelectionKey k (.keys selector)
              :let                               [state (.attachment k)]
              :when                              state]
        (when (= client-id (@state :client-id))
          ((:close-conn-fn deps) k))))))

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

(defn remove-idle-sessions
  [deps server]
  (let [timeout ((:idle-timeout-fn deps) server)
        clients ((:clients-fn deps) server)]
    (doseq [[client-id session] clients
            :let                [{:keys [last-active]} session]]
      (if last-active
        (when (< timeout (- (System/currentTimeMillis) ^long last-active))
          (disconnect-client* deps server client-id))
        ;; migrate old sessions that don't have last-active
        (update-client deps server client-id
                       #(assoc % :last-active (System/currentTimeMillis)))))))
