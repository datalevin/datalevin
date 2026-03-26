;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server.handlers
  "Wire/message handlers for the Datalevin server."
  (:refer-clojure :exclude [sync])
  (:require
   [clojure.string :as s]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ctrl]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.protocol :as p]
   [datalevin.server.api :as sapi]
   [datalevin.server.auth :as auth]
   [datalevin.storage :as st]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [java.nio.channels SelectionKey SocketChannel]
   [java.nio.file Paths]
   [java.util UUID]
   [java.util.concurrent Semaphore]))

(def ^:private view-act :datalevin.server/view)
(def ^:private alter-act :datalevin.server/alter)
(def ^:private create-act :datalevin.server/create)
(def ^:private control-act :datalevin.server/control)

(def ^:private database-obj :datalevin.server/database)
(def ^:private user-obj :datalevin.server/user)
(def ^:private role-obj :datalevin.server/role)
(def ^:private server-obj :datalevin.server/server)
(def ^:private transient-runtime-store-max-attempts 8)
(def ^:private transient-runtime-store-retry-sleep-ms 25)

(defn- skey-state
  ^clojure.lang.Volatile [^SelectionKey skey]
  (.attachment skey))

(defn- db-lock
  ^Semaphore [deps server db-name]
  ((:get-lock deps) server db-name))

(defn- state-lock
  ^Semaphore [dbs db-name]
  (get-in dbs [db-name :lock]))

(defn- with-error
  [deps skey f]
  (try
    (f)
    (catch Exception e
      ((:handle-message-error! deps) skey e))))

(defn- with-permission!
  [deps server ^SelectionKey skey req-act req-obj req-tgt denied-message f]
  (let [{:keys [client-id write-bf wire-opts]} @(skey-state skey)
        ^SocketChannel ch                    (.channel skey)
        {:keys [permissions]}                ((:get-client deps) server client-id)]
    (if permissions
      (if (auth/has-permission? req-act req-obj req-tgt permissions)
        (f)
        (u/raise denied-message {}))
      (do
        ((:remove-client deps) server client-id)
        (p/write-message-blocking ch write-bf {:type :reconnect} wire-opts)))))

(defn- sys-conn
  [deps server]
  ((:sys-conn deps) server))

(defn- db-state
  [deps server db-name]
  ((:db-state deps) server db-name))

(defn- dt-store
  [deps server skey db-name writing?]
  ((:store deps) server skey db-name writing?))

(defn- kv-store
  [deps server skey db-name writing?]
  ((:lmdb deps) server skey db-name writing?))

(defn- write-complete!
  [deps skey]
  ((:write-message deps) skey {:type :command-complete}))

(defn- write-result!
  [deps skey result]
  ((:write-message deps) skey {:type :command-complete :result result}))

(defn- with-runtime-store-read-access
  [deps server db-name f]
  (if-let [with-read-access (:with-db-runtime-store-read-access deps)]
    (with-read-access server db-name f)
    (f)))

(defn- with-best-effort-db-transaction-slot
  [deps server db-name f]
  (let [^Semaphore lock ((:get-lock deps) server db-name)]
    (if (.tryAcquire lock)
      (try
        (f)
        (finally
          (.release lock)))
      {:ok? false
       :skipped? true
       :reason :write-transaction-open
       :db-name db-name})))

(defn- transient-runtime-store-error?
  [e]
  (boolean
    (some
      (fn [cause]
        (let [message (or (ex-message cause) "")
              data    (or (ex-data cause) {})
              err-data (or (:err-data data) {})
              cause-msg (or (:cause data) (:cause err-data) "")]
          (or (s/includes? message
                           "Please do not open multiple LMDB connections")
              (s/includes? message "LMDB env is closed")
              (s/includes? message "Invalid argument")
              (and (string? cause-msg)
                   (s/includes? cause-msg "Invalid argument")))))
      (take-while some? (iterate ex-cause e)))))

(defn- with-transient-runtime-store-retry
  [f]
  (loop [attempt 0]
    (let [outcome (try
                    {:ok? true
                     :value (f)}
                    (catch Throwable e
                      {:ok? false
                       :error e}))]
      (if (:ok? outcome)
        (:value outcome)
        (let [e (:error outcome)
              attempt (long attempt)]
          (if (and (< attempt (long transient-runtime-store-max-attempts))
                   (transient-runtime-store-error? e))
            (let [attempt' (unchecked-inc attempt)]
              (Thread/sleep
               (unchecked-multiply
                (long transient-runtime-store-retry-sleep-ms)
                (long attempt')))
              (recur attempt'))
            (throw e)))))))

(defn- with-transient-runtime-store-skip
  [db-name op f]
  (try
    (with-transient-runtime-store-retry f)
    (catch Throwable e
      (if (transient-runtime-store-error? e)
        {:ok? false
         :skipped? true
         :reason :transient-runtime-store
         :operation op
         :db-name db-name}
        (throw e)))))

(defn- write-or-copy-result!
  [deps skey data]
  (if (coll? data)
    (if (< (count data) ^long c/+wire-datom-batch-size+)
      (write-result! deps skey data)
      ((:copy-out deps) skey data c/+wire-datom-batch-size+))
    (write-result! deps skey data)))

(defn- deserialize-arg
  [args idx]
  (let [frozen (nth args idx)]
    (replace {frozen (b/deserialize frozen)} args)))

(defn- normal-dt-handler
  [op]
  (fn [deps server skey {:keys [args writing?]}]
    (with-error
      deps
      skey
      #(write-result!
        deps
        skey
        (apply op
               (dt-store deps server skey (nth args 0) writing?)
               (rest args))))))

(defn- normal-kv-handler
  [op]
  (fn [deps server skey {:keys [args writing?]}]
    (with-error
      deps
      skey
      #(write-result!
        deps
        skey
        (apply op
               (kv-store deps server skey (nth args 0) writing?)
               (rest args))))))

(defn- copying-dt-handler
  [op]
  (fn [deps server skey {:keys [args writing?]}]
    (with-error
      deps
      skey
      #(write-or-copy-result!
        deps
        skey
        (apply op
               (dt-store deps server skey (nth args 0) writing?)
               (rest args))))))

(defn- copying-kv-handler
  [op]
  (fn [deps server skey {:keys [args writing?]}]
    (with-error
      deps
      skey
      #(write-or-copy-result!
        deps
        skey
        (apply op
               (kv-store deps server skey (nth args 0) writing?)
               (rest args))))))

(defn- deserialized-normal-dt-handler
  [idx op]
  (fn [deps server skey {:keys [args writing?]}]
    (with-error
      deps
      skey
      #(let [args (deserialize-arg args idx)]
         (write-result!
           deps
           skey
           (apply op
                  (dt-store deps server skey (nth args 0) writing?)
                  (rest args)))))))

(defn- deserialized-normal-kv-handler
  [idx op]
  (fn [deps server skey {:keys [args writing?]}]
    (with-error
      deps
      skey
      #(let [args (deserialize-arg args idx)]
         (write-result!
           deps
           skey
           (apply op
                  (kv-store deps server skey (nth args 0) writing?)
                  (rest args)))))))

(defn- deserialized-copying-dt-handler
  [idx op]
  (fn [deps server skey {:keys [args writing?]}]
    (with-error
      deps
      skey
      #(let [args (deserialize-arg args idx)]
         (write-or-copy-result!
           deps
           skey
           (apply op
                  (dt-store deps server skey (nth args 0) writing?)
                  (rest args)))))))

(defn- deserialized-copying-kv-handler
  [idx op]
  (fn [deps server skey {:keys [args writing?]}]
    (with-error
      deps
      skey
      #(let [args (deserialize-arg args idx)]
         (write-or-copy-result!
           deps
           skey
           (apply op
                  (kv-store deps server skey (nth args 0) writing?)
                  (rest args)))))))

(defn- db-alter-permission!
  [deps server skey db-name denied-message f]
  (with-permission!
    deps
    server
    skey
    alter-act
    database-obj
    (auth/db-eid (sys-conn deps server) db-name)
    denied-message
    f))

(defn- api-deps
  [deps]
  (select-keys deps
               [:copy-out
                :db-state
                :get-db
                :get-store
                :lmdb
                :search-engine
                :search-engine*
                :update-client
                :update-db
                :vector-index
                :write-message]))

(defn authentication
  [deps server skey message]
  (with-error
    deps
    skey
    #(if-let [client-id ((:authenticate deps) server skey message)]
       ((:write-message deps) skey
        {:type :authentication-ok :client-id client-id})
       (u/raise "Failed to authenticate" {}))))

(defn disconnect
  [deps server ^SelectionKey skey _]
  (let [{:keys [client-id]} @(skey-state skey)]
    ((:disconnect-client* deps) server client-id)))

(defn set-client-id
  [deps _server ^SelectionKey skey message]
  (let [client-id         (:client-id message)
        wire-capabilities (:wire-capabilities message)
        wire-opts         (p/negotiate-wire-opts wire-capabilities)]
    ((:write-message deps) skey
     {:type              :set-client-id-ok
      :wire-capabilities (p/local-wire-capabilities)})
    (vswap! (skey-state skey)
            assoc :client-id client-id :wire-opts wire-opts)))

(defn create-user
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn            (sys-conn deps server)
           [username password] args
           username            (u/lisp-case username)]
       (with-permission!
         deps server skey create-act user-obj nil
         "Don't have permission to create user"
         (fn []
           (if (s/blank? password)
             (u/raise "Password is required when creating user." {})
             (do
               (auth/transact-new-user sys-conn username password)
               ((:write-message deps) skey
                {:type :command-complete :username username}))))))))

(defn reset-password
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn            (sys-conn deps server)
           [username password] args
           uid                 (auth/user-eid sys-conn username)]
       (if uid
         (with-permission!
           deps server skey alter-act user-obj uid
           (str "Don't have permission to reset password of " username)
           (fn []
             (if (s/blank? password)
               (u/raise "New password is required when resetting password" {})
               (do
                 (auth/transact-new-password sys-conn username password)
                 (write-complete! deps skey)))))
         (u/raise "User does not exist" {:username username})))))

(defn drop-user
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn   (sys-conn deps server)
           [username] args
           uid        (auth/user-eid sys-conn username)]
       (if (= username c/default-username)
         (u/raise "Default user cannot be dropped." {})
         (if uid
           (with-permission!
             deps server skey create-act user-obj uid
             "Don't have permission to drop the user"
             (fn []
               ((:disconnect-user deps) server username)
               (auth/transact-drop-user sys-conn uid username)
               (write-complete! deps skey)))
           (u/raise "User does not exist." {:user username}))))))

(defn list-users
  [deps server skey _]
  (with-error
    deps
    skey
    #(with-permission!
       deps server skey view-act user-obj nil
       "Don't have permission to list users"
       (fn []
         (write-result! deps skey (auth/query-users (sys-conn deps server)))))))

(defn create-role
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [[role-key] args]
       (with-permission!
         deps server skey create-act role-obj nil
         "Don't have permission to create role"
         (fn []
           (auth/transact-new-role (sys-conn deps server) role-key)
           (write-complete! deps skey))))))

(defn drop-role
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn   (sys-conn deps server)
           [role-key] args
           rid        (auth/role-eid sys-conn role-key)]
       (if rid
         (if (auth/user-role-key? sys-conn role-key)
           (u/raise "Cannot drop default role of an active user" {})
           (with-permission!
             deps server skey create-act role-obj rid
             "Don't have permission to drop the role"
             (fn []
               (auth/transact-drop-role sys-conn rid)
               ((:update-cached-permission deps) server role-key)
               (write-complete! deps skey))))
         (u/raise "Role does not exist." {:role role-key})))))

(defn list-roles
  [deps server skey _]
  (with-error
    deps
    skey
    #(with-permission!
       deps server skey view-act role-obj nil
       "Don't have permission to list roles"
       (fn []
         (write-result! deps skey (auth/query-roles (sys-conn deps server)))))))

(defn create-database
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [[db-name db-type] args
           db-name           (u/lisp-case db-name)]
       (with-permission!
         deps server skey create-act database-obj nil
         "Don't have permission to create database"
         (fn []
           (if ((:db-exists? deps) server db-name)
             (u/raise "Database already exists." {:db db-name})
             (do
               ((:open-server-store deps) server skey
                {:db-name db-name :respond? false}
                db-type)
               nil))
           (write-complete! deps skey))))))

(defn close-database
  [deps server ^SelectionKey skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn            (sys-conn deps server)
           [db-name]           args
           {:keys [client-id]} @(skey-state skey)
           did                 (auth/db-eid sys-conn db-name)]
       (if did
         (if ((:get-store deps) server db-name)
           (with-permission!
             deps server skey create-act database-obj did
             "Don't have permission to close the database"
             (fn []
               (doseq [[cid {:keys [stores]}] ((:clients deps) server)
                       :when                  (get stores db-name)]
                 (when (not= client-id cid)
                   ((:disconnect-client* deps) server cid)))
               ((:remove-store deps) server db-name)
               (write-complete! deps skey)))
           (u/raise "Database is closed already." {}))
         (u/raise "Database doe snot exist." {})))))

(defn drop-database
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn  (sys-conn deps server)
           [db-name] args
           did       (auth/db-eid sys-conn db-name)]
       (if did
         (with-permission!
           deps server skey create-act database-obj did
           "Don't have permission to drop the database"
           (fn []
             (if ((:db-in-use? deps) server db-name)
               (u/raise "Cannot drop a database currently in use." {})
               (do
                 (auth/transact-drop-db sys-conn did)
                 (u/delete-files
                  ((:db-dir deps) ((:root deps) server) db-name))
                 (write-complete! deps skey)))))
         (u/raise "Database does not exist." {})))))

(defn list-databases
  [deps server skey _]
  (with-error
    deps
    skey
    #(with-permission!
       deps server skey create-act database-obj nil
       "Don't have permission to list databases"
       (fn []
         (write-result!
           deps skey (auth/query-databases (sys-conn deps server)))))))

(defn list-databases-in-use
  [deps server skey _]
  (with-error
    deps
    skey
    #(with-permission!
       deps server skey create-act database-obj nil
       "Don't have permission to list databases in use"
       (fn []
         (write-result! deps skey ((:in-use-dbs deps) server))))))

(defn assign-role
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn            (sys-conn deps server)
           [role-key username] args
           rid                 (auth/role-eid sys-conn role-key)]
       (if rid
         (with-permission!
           deps server skey alter-act role-obj rid
           "Don't have permission to assign the role to user"
           (fn []
             (auth/transact-user-role sys-conn rid username)
             ((:update-cached-role deps) server username)
             (write-complete! deps skey)))
         (u/raise "Role does not exist." {})))))

(defn withdraw-role
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn            (sys-conn deps server)
           [role-key username] args
           rid                 (auth/role-eid sys-conn role-key)]
       (if rid
         (if (auth/user-role-key? sys-conn role-key username)
           (u/raise "Cannot withdraw the default role of a user" {})
           (with-permission!
             deps server skey alter-act role-obj rid
             "Don't have permission to withdraw the role from user"
             (fn []
               (auth/transact-withdraw-role sys-conn rid username)
               ((:update-cached-role deps) server username)
               (write-complete! deps skey))))
         (u/raise "Role does not exist." {})))))

(defn list-user-roles
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn   (sys-conn deps server)
           [username] args
           uid        (auth/user-eid sys-conn username)]
       (if uid
         (with-permission!
           deps server skey view-act user-obj uid
           "Don't have permission to view the user's roles"
           (fn []
             (write-result! deps skey (auth/user-roles sys-conn username))))
         (u/raise "User does not exist." {})))))

(defn grant-permission
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn                              (sys-conn deps server)
           [role-key perm-act perm-obj perm-tgt] args
           rid                                   (auth/role-eid sys-conn role-key)]
       (if rid
         (with-permission!
           deps server skey alter-act role-obj rid
           "Don't have permission to grant permission to the role"
           (fn []
             (if (and (auth/permission-actions perm-act)
                      (auth/permission-objects perm-obj))
               (auth/transact-role-permission
                sys-conn rid perm-act perm-obj perm-tgt)
               (u/raise "Unknown permission action or object." {}))
             ((:update-cached-permission deps) server role-key)
             (write-complete! deps skey)))
         (u/raise "Role does not exist." {})))))

(defn revoke-permission
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn                              (sys-conn deps server)
           [role-key perm-act perm-obj perm-tgt] args
           rid                                   (auth/role-eid sys-conn role-key)]
       (if rid
         (with-permission!
           deps server skey alter-act role-obj rid
           "Don't have permission to revoke permission from the role"
           (fn []
             (auth/transact-revoke-permission
              sys-conn rid perm-act perm-obj perm-tgt)
             ((:update-cached-permission deps) server role-key)
             (write-complete! deps skey)))
         (u/raise "Role does not exist." {})))))

(defn list-role-permissions
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn   (sys-conn deps server)
           [role-key] args
           rid        (auth/role-eid sys-conn role-key)]
       (if rid
         (with-permission!
           deps server skey view-act role-obj rid
           "Don't have permission to list permissions of the role"
           (fn []
             (write-result!
               deps skey (auth/role-permissions sys-conn role-key))))
         (u/raise "Role does not exist." {})))))

(defn list-user-permissions
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [sys-conn   (sys-conn deps server)
           [username] args
           uid        (auth/user-eid sys-conn username)]
       (if uid
         (with-permission!
           deps server skey view-act user-obj uid
           "Don't have permission to list permission of the user"
           (fn []
             (write-result!
               deps skey (auth/user-permissions sys-conn username))))
         (u/raise "User does not exist." {})))))

(defn query-system
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [[query arguments] args]
       (with-permission!
         deps server skey view-act server-obj nil
         "Don't have permission to query system."
         (fn []
           (write-result! deps skey
                          (apply d/q query @(sys-conn deps server)
                                 arguments)))))))

(defn show-clients
  [deps server skey _]
  (with-error
    deps
    skey
    #(with-permission!
       deps server skey view-act server-obj nil
       "Don't have permission to show clients."
       (fn []
         (write-result!
           deps
           skey
           (->> ((:clients deps) server)
                (map (partial (:client-display deps) server))
                (into {})))))))

(defn disconnect-client
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [[cid] args]
       (with-permission!
         deps server skey control-act server-obj nil
         "Don't have permission to disconnect a client"
         (fn []
           ((:disconnect-client* deps) server cid)
           (write-complete! deps skey))))))

(defn open
  [deps server skey message]
  ((:open-server-store deps) server skey message c/dl-type))

(defn close
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(do
       ((:detach-client-store! deps) server skey (nth args 0))
       (write-complete! deps skey))))

(defn closed?
  [deps server skey {:keys [args writing?]}]
  (with-error
    deps
    skey
    #(let [db-name (nth args 0)
           res     (if-let [s (dt-store deps server skey db-name writing?)]
                     ((:store-closed? deps) s)
                     true)]
       (write-result! deps skey res))))

(defn assoc-opt
  [deps server skey {:keys [args writing?]}]
  (with-error
    deps
    skey
    #(let [db-name (nth args 0)
           store   (dt-store deps server skey db-name writing?)
           [k v]   (rest args)
           result  ((:apply-assoc-opt! deps)
                    server db-name store writing? k v)]
       (write-result! deps skey result))))

(defn set-schema
  [deps server skey {:keys [args writing?]}]
  (with-error
    deps
    skey
    #(let [db-name (nth args 0)
           db-name ((:store->db-name deps)
                    server
                    ((:db-store deps) server skey db-name))]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (write-result!
             deps
             skey
             (apply i/set-schema
                    (dt-store deps server skey (nth args 0) writing?)
                    (rest args))))))))

(defn load-datoms
  [deps server skey {:keys [mode args writing?]}]
  (with-error
    deps
    skey
    #(let [db-name (nth args 0)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (case mode
             :copy-in
             (let [dt-store (dt-store deps server skey db-name writing?)]
               (i/load-datoms dt-store ((:copy-in deps) server skey))
               (write-complete! deps skey))

             :request
             (write-result!
               deps
               skey
               (apply i/load-datoms
                      (dt-store deps server skey db-name writing?)
                      (rest args)))

             (u/raise "Missing :mode when loading datoms" {})))))))

(defn- transact*
  [deps db0 txs s? server db-name writing?]
  (try
    (d/with db0 txs {} s?)
    (catch Exception e
      (when (:resized (ex-data e))
        (let [new-db (db/carry-runtime-opts
                      (db/new-db ((:get-store deps) server db-name writing?))
                      db0)]
          ((:update-db deps) server db-name
           (fn [m]
             (assoc m (if writing? :wdt-db :dt-db) new-db)))))
      (throw e))))

(defn tx-data
  [deps server skey {:keys [mode args writing?]}]
  (with-error
    deps
    skey
    #(let [db-name (nth args 0)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (let [txs (case mode
                       :copy-in ((:copy-in deps) server skey)
                       :request (nth args 1)
                       (u/raise "Missing :mode when transact data" {}))
                 db0 (get-in (db-state deps server db-name)
                             [(if writing? :wdt-db :dt-db)])
                 s?  (last args)
                 rp  (transact* deps db0 txs s? server db-name writing?)
                 db1 (:db-after rp)
                 _   ((:update-db deps) server db-name
                      (fn [m]
                        (assoc m (if writing? :wdt-db :dt-db) db1)))
                 rp  (assoc-in rp [:tempids :max-eid] (:max-eid db1))
                 ct  (+ (count (:tx-data rp)) (count (:tempids rp)))
                 res (cond-> (select-keys rp [:tx-data :tempids])
                       (:new-attributes rp)
                       (assoc :new-attributes (:new-attributes rp)))]
             (if (< ct ^long c/+wire-datom-batch-size+)
               (write-result! deps skey res)
               (let [{:keys [tx-data tempids]} res
                     response-meta            (dissoc res :tx-data :tempids)]
                 ((:copy-out deps) skey (into tx-data tempids)
                  c/+wire-datom-batch-size+ nil response-meta)))))))))

(defn db-info
  [deps server skey {:keys [args writing?]}]
  (with-error
    deps
    skey
    #(let [db-name  (nth args 0)
           dt-store (dt-store deps server skey db-name writing?)]
       (write-result! deps skey
                      {:max-eid       (i/init-max-eid dt-store)
                       :max-tx        (i/max-tx dt-store)
                       :last-modified (i/last-modified dt-store)
                       :opts          (i/opts dt-store)}))))

(defn tx-data+db-info
  [deps server skey {:keys [mode args writing?]}]
  (with-error
    deps
    skey
    #(let [db-name (nth args 0)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (let [txs (case mode
                       :copy-in ((:copy-in deps) server skey)
                       :request (nth args 1)
                       (u/raise "Missing :mode when transact data" {}))
                 db0 (get-in (db-state deps server db-name)
                             [(if writing? :wdt-db :dt-db)])
                 s?  (last args)
                 rp  (transact* deps db0 txs s? server db-name writing?)
                 db1 (:db-after rp)
                 _   ((:update-db deps) server db-name
                      (fn [m]
                        (assoc m (if writing? :wdt-db :dt-db) db1)))
                 rp  (assoc-in rp [:tempids :max-eid] (:max-eid db1))
                 dt-store (dt-store deps server skey db-name writing?)
                 db-info  {:max-eid       (:max-eid db1)
                           :max-tx        (i/max-tx dt-store)
                           :last-modified (i/last-modified dt-store)}
                 ct  (+ (count (:tx-data rp)) (count (:tempids rp)))
                 res (cond-> (select-keys rp [:tx-data :tempids])
                       (:new-attributes rp)
                       (assoc :new-attributes (:new-attributes rp))
                       true
                       (assoc :db-info db-info))]
             (if (< ct ^long c/+wire-datom-batch-size+)
               (write-result! deps skey res)
               (let [{:keys [tx-data tempids]} res
                     response-meta            (dissoc res :tx-data :tempids)]
                 ((:copy-out deps) skey (into tx-data tempids)
                  c/+wire-datom-batch-size+ nil response-meta)))))))))

(defn open-kv
  [deps server skey message]
  ((:open-server-store deps) server skey message c/kv-type))

(defn close-kv
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(do
       ((:detach-client-store! deps) server skey (nth args 0))
       (write-complete! deps skey))))

(defn open-dbi
  [deps server ^SelectionKey skey {:keys [args writing?]}]
  (with-error
    deps
    skey
    #(let [{:keys [client-id]} @(skey-state skey)
           db-name             (nth args 0)
           kv                  (kv-store deps server skey db-name writing?)
           args                (rest args)
           dbi-name            (first args)]
       (apply i/open-dbi kv args)
       ((:update-client deps) server client-id
        (fn [m]
          (update-in m [:stores db-name :dbis] conj dbi-name)))
       (write-complete! deps skey))))

(defn drop-dbi
  [deps server ^SelectionKey skey {:keys [args writing?]}]
  (with-error
    deps
    skey
    #(let [{:keys [client-id]} @(skey-state skey)
           db-name             (nth args 0)
           kv                  (kv-store deps server skey db-name writing?)
           args                (rest args)
           dbi-name            (first args)]
       (i/drop-dbi kv dbi-name)
       ((:update-client deps) server client-id
        (fn [m]
          (update-in m [:stores db-name :dbis] disj dbi-name)))
       (write-complete! deps skey))))

(defn- best-effort-unpin-server-copy-backup-floor!
  [deps db-name source-store copy-backup-pin]
  (when-let [pin-id (:pin-id copy-backup-pin)]
    (try
      ((:unpin-server-copy-backup-floor! deps) source-store pin-id)
      (catch Throwable e
        (log/debug e
                   "Best-effort server copy backup pin cleanup failed"
                   {:db-name db-name
                    :pin-id pin-id})))))

(defn copy
  [deps server skey {:keys [args writing?]}]
  (with-error
    deps
    skey
    #(let [[db-name compact?] args
           started-ms         (System/currentTimeMillis)
           copy-backup-pin    (atom nil)
           source-store-v     (volatile! nil)
           tf                 (u/tmp-dir (str "copy-" (UUID/randomUUID)))
           path               (Paths/get (str tf u/+separator+ c/data-file-name)
                                         (into-array String []))]
       (letfn [(cleanup-copy-dir! []
                 (try
                   ((:cleanup-copy-tmp-dir! deps) tf)
                   (catch Throwable _ nil))
                 (u/create-dirs tf))
               (copy-store! [source-store backup-pin-enabled?]
                 (reset! copy-backup-pin nil)
                 (binding [kv/*wal-copy-backup-pin-observer*
                           (when backup-pin-enabled?
                             (fn [{:keys [pin-id pin-floor-lsn pin-expires-ms]}]
                               (reset! copy-backup-pin
                                       {:pin-id pin-id
                                        :floor-lsn pin-floor-lsn
                                        :expires-ms pin-expires-ms})))
                           kv/*wal-copy-backup-pin-enabled?*
                           backup-pin-enabled?]
                   ((:server-copy-store! deps) source-store tf compact?)))]
       (try
         (with-runtime-store-read-access
           deps
           server
           db-name
           (fn []
             ;; Snapshot copy must not race a runtime store swap/reopen.
             (with-transient-runtime-store-retry
               (fn []
                 (let [source-store (kv-store deps server skey db-name writing?)]
                   (vreset! source-store-v source-store)
                   (try
                     (copy-store! source-store true)
                     (catch Throwable e
                       (when (transient-runtime-store-error? e)
                         (cleanup-copy-dir!)
                         (log/debug
                           e
                           "Retrying server copy without WAL backup pin after transient source-store race"
                           {:db-name db-name})
                         (copy-store! source-store false))
                       (throw e))))))))
         (let [completed-ms (System/currentTimeMillis)
               copied-store ((:open-server-copied-store! deps) tf nil nil)]
           (try
             (let [copy-meta ((:copy-response-meta deps)
                              db-name
                              copied-store
                              (cond-> {:started-ms started-ms
                                       :completed-ms completed-ms
                                       :duration-ms (- completed-ms started-ms)
                                       :compact? (boolean compact?)}
                                (map? @copy-backup-pin)
                                (assoc :backup-pin @copy-backup-pin)))]
               ((:copy-server-file-out! deps) skey path copy-meta))
             (finally
               (when-not (i/closed? copied-store)
                 ((:close-server-copied-store! deps) copied-store)))))
         (finally
           (best-effort-unpin-server-copy-backup-floor!
            deps db-name @source-store-v @copy-backup-pin)
           (try
             ((:cleanup-copy-tmp-dir! deps) tf)
             (catch Throwable e
               (log/warn e
                         "Unable to delete temporary copy directory"
                         {:path (str tf)})))))))))

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

(defn batch-kv
  [deps server skey {:keys [args writing?]}]
  (with-error
    deps
    skey
    #(let [[db-name calls] args
           kv-store        (kv-store deps server skey db-name writing?)]
       (when-not (sequential? calls)
         (u/raise "batch-kv calls must be a sequential collection"
                  {:calls calls}))
       (write-result!
         deps
         skey
         (mapv
           (fn [call]
             (when-not (sequential? call)
               (u/raise "Each batch-kv call must be a vector [op & args]"
                        {:call call}))
             (run-batch-kv-call kv-store call))
           calls)))))

(defn open-transact-kv
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    (fn []
      (let [db-name          (nth args 0)
            ^Semaphore lock (db-lock deps server db-name)]
        (db-alter-permission!
          deps server skey db-name
          "Don't have permission to alter the database"
          (fn []
            (.acquire lock)
            (try
              (let [{:keys [kv-store wlmdb]}
                    ((:open-write-txn-with-retry deps) server db-name)]
                ((:update-db deps) server db-name
                 (fn [m]
                   (assoc m :wlmdb wlmdb)))
                (let [runner ((:write-txn-runner deps) server db-name kv-store)]
                  (write-complete! deps skey)
                  ((:run-calls deps) runner)))
              (catch Throwable t
                (.release lock)
                (throw t)))))))))

(defn close-transact-kv
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    (fn []
      (let [db-name          (nth args 0)
            kv-store         ((:get-kv-store deps) server db-name)
            dbs              ((:dbs deps) server)
            ^Semaphore lock (state-lock dbs db-name)]
        (db-alter-permission!
          deps server skey db-name
          "Don't have permission to alter the database"
          (fn []
            (try
              (i/close-transact-kv kv-store)
              (write-complete! deps skey)
              (finally
                ((:halt-run deps) (get-in dbs [db-name :runner]))
                ((:update-db deps) server db-name
                 (fn [m]
                   (dissoc m :runner :wlmdb)))
                (.release lock))))))))

(defn abort-transact-kv
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    (fn []
      (let [db-name          (nth args 0)
            kv-store         ((:get-kv-store deps) server db-name)
            dbs              ((:dbs deps) server)
            ^Semaphore lock (state-lock dbs db-name)]
        (db-alter-permission!
          deps server skey db-name
          "Don't have permission to alter the database"
          (fn []
            (try
              (i/abort-transact-kv kv-store)
              (i/close-transact-kv kv-store)
              (finally
                ((:halt-run deps) (get-in dbs [db-name :runner]))
                ((:update-db deps) server db-name
                 (fn [m]
                   (dissoc m :runner :wlmdb)))
                (.release lock)))
            (write-complete! deps skey)))))))

(defn open-transact
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    (fn []
      (let [db-name          (nth args 0)
            ^Semaphore lock (db-lock deps server db-name)]
        (db-alter-permission!
          deps server skey db-name
          "Don't have permission to alter the database"
          (fn []
            (.acquire lock)
            (try
              (let [{:keys [store kv-store wlmdb]}
                    ((:open-write-txn-with-retry deps) server db-name)
                    wstore (st/transfer store wlmdb)
                    runner ((:write-txn-runner deps) server db-name kv-store)]
                ((:update-db deps)
                 server
                 db-name
                 (fn [m]
                   (assoc m
                          :wlmdb wlmdb
                          :wstore wstore
                          :wdt-db ((:new-runtime-db deps)
                                   wstore
                                   ((:current-runtime-opts deps) m)))))
                (write-complete! deps skey)
                ((:run-calls deps) runner))
              (catch Throwable t
                (.release lock)
                (throw t)))))))))

(defn close-transact
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    (fn []
      (let [db-name          (nth args 0)
            kv-store         ((:get-kv-store deps) server db-name)
            dbs              ((:dbs deps) server)
            ^Semaphore lock (state-lock dbs db-name)]
        (db-alter-permission!
          deps server skey db-name
          "Don't have permission to alter the database"
          (fn []
            (try
              (i/close-transact-kv kv-store)
              ((:add-store deps)
               server db-name
               (st/transfer (get-in dbs [db-name :wstore]) kv-store))
              (write-complete! deps skey)
              (finally
                ((:halt-run deps) (get-in dbs [db-name :runner]))
                ((:update-db deps) server db-name
                 (fn [m]
                   (dissoc m :wlmdb :wstore :wdt-db :runner)))
                (.release lock)))))))))

(defn abort-transact
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    (fn []
      (let [db-name  (nth args 0)
            kv-store ((:get-kv-store deps) server db-name)
            dbs      ((:dbs deps) server)
            ^Semaphore lock (state-lock dbs db-name)]
        (db-alter-permission!
          deps server skey db-name
          "Don't have permission to alter the database"
          (fn []
            (try
              (i/abort-transact-kv kv-store)
              (i/close-transact-kv kv-store)
              (finally
                ((:halt-run deps) (get-in dbs [db-name :runner]))
                ((:update-db deps) server db-name
                 (fn [m]
                   (dissoc m :wlmdb :wstore :wdt-db :runner)))
                (.release lock)))
            (write-complete! deps skey))))))))

(defn sync
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [db-name  (nth args 0)
           force    (nth args 1)
           kv-store ((:get-kv-store deps) server db-name)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (i/sync kv-store force)
           (write-complete! deps skey))))))

(defn ha-watermark
  [deps server skey {:keys [args writing?]}]
  (with-error
    deps
    skey
    #(let [db-name          (nth args 0)
           kv-store         (kv-store deps server skey db-name writing?)
           db-state         (db-state deps server db-name)
           authority        (:ha-authority db-state)
           txlog-watermarks (try
                              (with-transient-runtime-store-retry
                                (fn []
                                  (kv/txlog-watermarks kv-store)))
                              (catch Throwable e
                                (when-not (transient-runtime-store-error? e)
                                  (throw e))
                                nil))
           authority-diag   (when authority
                              (try
                                (ctrl/authority-diagnostics authority)
                                (catch Throwable _
                                  nil)))
           txlog-lsn        (long (or (:last-applied-lsn txlog-watermarks) 0))
           runtime-lsn      (when authority
                              (try
                                (long (with-transient-runtime-store-retry
                                        (fn []
                                          (dha/read-ha-local-last-applied-lsn
                                            db-state))))
                                (catch Throwable e
                                  (when-not (transient-runtime-store-error? e)
                                    (throw e))
                                  (long (or (:ha-local-last-applied-lsn
                                             db-state)
                                            0)))))
           effective-lsn    (long (or runtime-lsn txlog-lsn))]
       (write-result!
         deps
         skey
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
                  (some-> (:node-state authority-diag) str)))))))

(defn force-txlog-sync!
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [db-name  (nth args 0)
           kv-store ((:get-kv-store deps) server db-name)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (write-result! deps skey (kv/force-txlog-sync! kv-store)))))))

(defn force-lmdb-sync!
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [db-name  (nth args 0)
           kv-store ((:get-kv-store deps) server db-name)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (write-result! deps skey (kv/force-lmdb-sync! kv-store)))))))

(defn create-snapshot!
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [db-name  (nth args 0)
           kv-store ((:get-kv-store deps) server db-name)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (write-result! deps skey (kv/create-snapshot! kv-store)))))))

(defn gc-txlog-segments!
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [db-name          (nth args 0)
           retain-floor-lsn (nth args 1 nil)
           kv-store         ((:get-kv-store deps) server db-name)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (write-result!
             deps skey (kv/gc-txlog-segments! kv-store retain-floor-lsn)))))))

(defn txlog-update-snapshot-floor!
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [db-name               (nth args 0)
           snapshot-lsn          (nth args 1)
           previous-snapshot-lsn (nth args 2 nil)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (write-result!
             deps
             skey
             (with-runtime-store-read-access
               deps
               server
               db-name
               (fn []
                 (kv/txlog-update-snapshot-floor!
                  ((:get-kv-store deps) server db-name)
                  snapshot-lsn
                  previous-snapshot-lsn)))))))))

(defn txlog-clear-snapshot-floor!
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [db-name (nth args 0)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (write-result!
             deps
             skey
             (with-runtime-store-read-access
               deps
               server
               db-name
               (fn []
                 (kv/txlog-clear-snapshot-floor!
                  ((:get-kv-store deps) server db-name))))))))))

(defn txlog-update-replica-floor!
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [db-name     (nth args 0)
           replica-id  (nth args 1)
           applied-lsn (nth args 2)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (write-result!
             deps
             skey
             (with-runtime-store-read-access
               deps
               server
               db-name
               (fn []
                 (with-transient-runtime-store-skip
                   db-name
                   :txlog-update-replica-floor!
                   (fn []
                     (with-best-effort-db-transaction-slot
                       deps
                       server
                       db-name
                       (fn []
                         (kv/txlog-update-replica-floor!
                          ((:get-kv-store deps) server db-name)
                          replica-id
                          applied-lsn)))))))))))))

(defn txlog-clear-replica-floor!
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [db-name    (nth args 0)
           replica-id (nth args 1)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (write-result!
             deps
             skey
             (with-runtime-store-read-access
               deps
               server
               db-name
               (fn []
                 (with-transient-runtime-store-skip
                   db-name
                   :txlog-clear-replica-floor!
                   (fn []
                     (with-best-effort-db-transaction-slot
                       deps
                       server
                       db-name
                       (fn []
                         (kv/txlog-clear-replica-floor!
                          ((:get-kv-store deps) server db-name)
                          replica-id)))))))))))))

(defn txlog-pin-backup-floor!
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [db-name    (nth args 0)
           pin-id     (nth args 1)
           floor-lsn  (nth args 2)
           expires-ms (nth args 3 nil)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (write-result!
             deps
             skey
             (with-runtime-store-read-access
               deps
               server
               db-name
               (fn []
                 (kv/txlog-pin-backup-floor!
                  ((:get-kv-store deps) server db-name)
                  pin-id
                  floor-lsn
                  expires-ms)))))))))

(defn txlog-unpin-backup-floor!
  [deps server skey {:keys [args]}]
  (with-error
    deps
    skey
    #(let [db-name (nth args 0)
           pin-id  (nth args 1)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (write-result!
             deps
             skey
             (with-runtime-store-read-access
               deps
               server
               db-name
               (fn []
                 (kv/txlog-unpin-backup-floor!
                  ((:get-kv-store deps) server db-name)
                  pin-id)))))))))

(defn transact-kv
  [deps server skey {:keys [mode args writing?]}]
  (with-error
    deps
    skey
    #(let [db-name  (nth args 0)
           kv-store ((:get-store deps) server db-name)]
       (db-alter-permission!
         deps server skey db-name
         "Don't have permission to alter the database"
         (fn []
           (case mode
             :copy-in
             (let [txs ((:copy-in deps) server skey)]
               (case (count args)
                 1 (i/transact-kv kv-store txs)
                 2 (i/transact-kv kv-store (nth args 1) txs)
                 3 (i/transact-kv kv-store (nth args 1) txs (nth args 2))
                 4 (i/transact-kv kv-store (nth args 1) txs
                                  (nth args 2) (nth args 3))
                 5 (i/transact-kv kv-store (nth args 1) txs
                                  (nth args 3) (nth args 4))
                 (u/raise "Invalid :args for transact-kv copy-in"
                          {:args args :mode mode}))
               (write-complete! deps skey))

             :request
             (write-result!
               deps
               skey
               (apply i/transact-kv kv-store (rest args)))

             (u/raise "Missing :mode when transacting kv" {})))))))

(defn q
  [deps server skey message]
  (with-error deps skey #(sapi/q (api-deps deps) server skey message)))

(defn pull
  [deps server skey message]
  (with-error deps skey #(sapi/pull (api-deps deps) server skey message)))

(defn pull-many
  [deps server skey message]
  (with-error deps skey #(sapi/pull-many (api-deps deps) server skey message)))

(defn explain
  [deps server skey message]
  (with-error deps skey #(sapi/explain (api-deps deps) server skey message)))

(defn fulltext-datoms
  [deps server skey message]
  (with-error
    deps skey #(sapi/fulltext-datoms (api-deps deps) server skey message)))

(defn new-search-engine
  [deps server ^SelectionKey skey message]
  (with-error
    deps
    skey
    #(sapi/new-search-engine
      (api-deps deps)
      server
      skey
      (:client-id @(skey-state skey))
      message)))

(defn add-doc
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/search-call (api-deps deps) server skey {:args args}
                                 i/add-doc)))

(defn remove-doc
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/search-call (api-deps deps) server skey {:args args}
                                 i/remove-doc)))

(defn clear-docs
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/search-call (api-deps deps) server skey {:args args}
                                 i/clear-docs)))

(defn doc-indexed?
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/search-call (api-deps deps) server skey {:args args}
                                 i/doc-indexed?)))

(defn doc-count
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/search-call (api-deps deps) server skey {:args args}
                                 i/doc-count)))

(defn search
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/search-call (api-deps deps) server skey {:args args}
                                 i/search)))

(defn search-re-index
  [deps server skey {:keys [args] :as _message}]
  (with-error
    deps skey #(sapi/search-re-index (api-deps deps) server skey {:args args})))

(defn new-vector-index
  [deps server ^SelectionKey skey message]
  (with-error
    deps
    skey
    #(sapi/new-vector-index
      (api-deps deps)
      server
      skey
      (:client-id @(skey-state skey))
      message)))

(defn add-vec
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/vector-call (api-deps deps) server skey {:args args}
                                 i/add-vec)))

(defn remove-vec
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/vector-call (api-deps deps) server skey {:args args}
                                 i/remove-vec)))

(defn persist-vecs
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/vector-call (api-deps deps) server skey {:args args}
                                 i/persist-vecs)))

(defn close-vecs
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/vector-call (api-deps deps) server skey {:args args}
                                 i/close-vecs)))

(defn clear-vecs
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/vector-call (api-deps deps) server skey {:args args}
                                 i/clear-vecs)))

(defn vec-indexed?
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/vector-call (api-deps deps) server skey {:args args}
                                 i/vec-indexed?)))

(defn vecs-info
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/vector-call (api-deps deps) server skey {:args args}
                                 i/vecs-info)))

(defn search-vec
  [deps server skey {:keys [args] :as _message}]
  (with-error deps skey
              #(sapi/vector-call (api-deps deps) server skey {:args args}
                                 i/search-vec)))

(defn vec-re-index
  [deps server skey {:keys [args] :as _message}]
  (with-error
    deps skey #(sapi/vec-re-index (api-deps deps) server skey {:args args})))

(defn kv-re-index
  [deps server skey {:keys [args] :as _message}]
  (with-error
    deps skey #(sapi/kv-re-index (api-deps deps) server skey {:args args})))

(defn datalog-re-index
  [deps server skey {:keys [args] :as _message}]
  (with-error
    deps skey
    #(sapi/datalog-re-index (api-deps deps) server skey {:args args})))

(def handler-map
  {:authentication authentication
   :disconnect disconnect
   :set-client-id set-client-id
   :create-user create-user
   :reset-password reset-password
   :drop-user drop-user
   :list-users list-users
   :create-role create-role
   :drop-role drop-role
   :list-roles list-roles
   :create-database create-database
   :close-database close-database
   :drop-database drop-database
   :list-databases list-databases
   :list-databases-in-use list-databases-in-use
   :assign-role assign-role
   :withdraw-role withdraw-role
   :list-user-roles list-user-roles
   :grant-permission grant-permission
   :revoke-permission revoke-permission
   :list-role-permissions list-role-permissions
   :list-user-permissions list-user-permissions
   :query-system query-system
   :show-clients show-clients
   :disconnect-client disconnect-client
   :open open
   :close close
   :closed? closed?
   :opts (normal-dt-handler i/opts)
   :assoc-opt assoc-opt
   :last-modified (normal-dt-handler i/last-modified)
   :schema (normal-dt-handler i/schema)
   :rschema (normal-dt-handler i/rschema)
   :set-schema set-schema
   :init-max-eid (normal-dt-handler i/init-max-eid)
   :max-tx (normal-dt-handler i/max-tx)
   :swap-attr (deserialized-normal-dt-handler 2 i/swap-attr)
   :del-attr (normal-dt-handler i/del-attr)
   :rename-attr (normal-dt-handler i/rename-attr)
   :datom-count (normal-dt-handler i/datom-count)
   :load-datoms load-datoms
   :tx-data tx-data
   :db-info db-info
   :tx-data+db-info tx-data+db-info
   :open-transact open-transact
   :close-transact close-transact
   :abort-transact abort-transact
   :set-env-flags (normal-kv-handler i/set-env-flags)
   :get-env-flags (normal-kv-handler i/get-env-flags)
   :sync sync
   :ha-watermark ha-watermark
   :txlog-watermarks (normal-kv-handler kv/txlog-watermarks)
   :open-tx-log (normal-kv-handler kv/open-tx-log)
   :open-tx-log-rows (normal-kv-handler kv/open-tx-log-rows)
   :read-commit-marker (normal-kv-handler kv/read-commit-marker)
   :verify-commit-marker! (normal-kv-handler kv/verify-commit-marker!)
   :force-txlog-sync! force-txlog-sync!
   :force-lmdb-sync! force-lmdb-sync!
   :create-snapshot! create-snapshot!
   :list-snapshots (normal-kv-handler kv/list-snapshots)
   :snapshot-scheduler-state (normal-kv-handler kv/snapshot-scheduler-state)
   :txlog-retention-state (normal-kv-handler kv/txlog-retention-state)
   :gc-txlog-segments! gc-txlog-segments!
   :txlog-update-snapshot-floor! txlog-update-snapshot-floor!
   :txlog-clear-snapshot-floor! txlog-clear-snapshot-floor!
   :txlog-update-replica-floor! txlog-update-replica-floor!
   :txlog-clear-replica-floor! txlog-clear-replica-floor!
   :txlog-pin-backup-floor! txlog-pin-backup-floor!
   :txlog-unpin-backup-floor! txlog-unpin-backup-floor!
   :fetch (normal-dt-handler i/fetch)
   :populated? (normal-dt-handler i/populated?)
   :size (normal-dt-handler i/size)
   :head (normal-dt-handler i/head)
   :tail (normal-dt-handler i/tail)
   :slice (copying-dt-handler i/slice)
   :rslice (copying-dt-handler i/rslice)
   :start-sampling (normal-dt-handler i/start-sampling)
   :stop-sampling (normal-dt-handler i/stop-sampling)
   :analyze (normal-dt-handler i/analyze)
   :e-datoms (copying-dt-handler i/e-datoms)
   :e-first-datom (normal-dt-handler i/e-first-datom)
   :av-datoms (copying-dt-handler i/av-datoms)
   :av-first-datom (normal-dt-handler i/av-first-datom)
   :av-first-e (normal-dt-handler i/av-first-e)
   :ea-first-datom (normal-dt-handler i/ea-first-datom)
   :ea-first-v (normal-dt-handler i/ea-first-v)
   :v-datoms (copying-dt-handler i/v-datoms)
   :size-filter (deserialized-normal-dt-handler 2 i/size-filter)
   :head-filter (deserialized-normal-dt-handler 2 i/head-filter)
   :tail-filter (deserialized-normal-dt-handler 2 i/tail-filter)
   :slice-filter (deserialized-copying-dt-handler 2 i/slice-filter)
   :rslice-filter (deserialized-copying-dt-handler 2 i/rslice-filter)
   :open-kv open-kv
   :close-kv close-kv
   :closed-kv? (normal-kv-handler i/closed-kv?)
   :open-dbi open-dbi
   :clear-dbi (normal-kv-handler i/clear-dbi)
   :drop-dbi drop-dbi
   :list-dbis (normal-kv-handler i/list-dbis)
   :copy copy
   :stat (normal-kv-handler i/stat)
   :entries (normal-kv-handler i/entries)
   :open-transact-kv open-transact-kv
   :close-transact-kv close-transact-kv
   :abort-transact-kv abort-transact-kv
   :transact-kv transact-kv
   :get-value (normal-kv-handler i/get-value)
   :get-rank (normal-kv-handler i/get-rank)
   :get-by-rank (normal-kv-handler i/get-by-rank)
   :sample-kv (normal-kv-handler i/sample-kv)
   :get-first (normal-kv-handler i/get-first)
   :get-first-n (normal-kv-handler i/get-first-n)
   :batch-kv batch-kv
   :key-range (copying-kv-handler i/key-range)
   :key-range-count (normal-kv-handler i/key-range-count)
   :key-range-list-count (normal-kv-handler i/key-range-list-count)
   :visit-key-range (deserialized-normal-kv-handler 2 i/visit-key-range)
   :get-range (copying-kv-handler i/get-range)
   :range-count (normal-kv-handler i/range-count)
   :get-some (deserialized-normal-kv-handler 2 i/get-some)
   :range-filter (deserialized-copying-kv-handler 2 i/range-filter)
   :range-keep (deserialized-copying-kv-handler 2 i/range-keep)
   :range-some (deserialized-normal-kv-handler 2 i/range-some)
   :range-filter-count (deserialized-normal-kv-handler 2 i/range-filter-count)
   :visit (deserialized-normal-kv-handler 2 i/visit)
   :get-list (copying-kv-handler i/get-list)
   :visit-list (deserialized-normal-kv-handler 2 i/visit-list)
   :list-count (normal-kv-handler i/list-count)
   :in-list? (normal-kv-handler i/in-list?)
   :list-range (copying-kv-handler i/list-range)
   :list-range-count (normal-kv-handler i/list-range-count)
   :list-range-first (normal-kv-handler i/list-range-first)
   :list-range-first-n (normal-kv-handler i/list-range-first-n)
   :list-range-filter (deserialized-copying-kv-handler 2 i/list-range-filter)
   :list-range-some (deserialized-normal-kv-handler 2 i/list-range-some)
   :list-range-keep (deserialized-copying-kv-handler 2 i/list-range-keep)
   :list-range-filter-count
   (deserialized-normal-kv-handler 2 i/list-range-filter-count)
   :visit-list-range (deserialized-normal-kv-handler 2 i/visit-list-range)
   :q q
   :pull pull
   :pull-many pull-many
   :explain explain
   :fulltext-datoms fulltext-datoms
   :new-search-engine new-search-engine
   :add-doc add-doc
   :remove-doc remove-doc
   :clear-docs clear-docs
   :doc-indexed? doc-indexed?
   :doc-count doc-count
   :search search
   :search-re-index search-re-index
   :new-vector-index new-vector-index
   :add-vec add-vec
   :remove-vec remove-vec
   :persist-vecs persist-vecs
   :close-vecs close-vecs
   :clear-vecs clear-vecs
   :vecs-info vecs-info
   :vec-indexed? vec-indexed?
   :search-vec search-vec
   :vec-re-index vec-re-index
   :kv-re-index kv-re-index
   :datalog-re-index datalog-re-index})
