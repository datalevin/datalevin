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
   [datalevin.lmdb :as l]
   [datalevin.protocol :as p]
   [datalevin.storage :as st]
   [datalevin.search :as sc]
   [datalevin.vector :as v]
   [datalevin.kv :as kv]
   [datalevin.built-ins :as dbq]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [taoensso.timbre :as log]
   [clojure.stacktrace :as stt]
   [clojure.string :as s])
  (:import
   [java.nio.charset StandardCharsets]
   [java.nio ByteBuffer BufferOverflowException]
   [java.nio.file Files Paths OpenOption]
   [java.nio.channels Selector SelectionKey ServerSocketChannel SocketChannel]
   [java.net InetSocketAddress]
   [java.security SecureRandom MessageDigest]
   [java.util Iterator UUID Map]
   [java.util.concurrent.atomic AtomicBoolean]
   [java.util.concurrent Executors Executor ExecutorService
    ConcurrentLinkedQueue ConcurrentHashMap Semaphore]
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.interface ILMDB IStore]
   [org.bouncycastle.crypto.generators Argon2BytesGenerator]
   [org.bouncycastle.crypto.params Argon2Parameters Argon2Parameters$Builder]))

(defprotocol IServer
  (start [srv] "Start the server")
  (stop [srv] "Stop the server"))

;; system db management

(def server-schema
  (merge c/implicit-schema
         c/entity-time-schema
         {:user/name    {:db/doc       "User name, must be unique"
                         :db/unique    :db.unique/identity
                         :db/valueType :db.type/string}
          :user/pw-hash {:db/doc       "Hash of password"
                         :db/valueType :db.type/string}
          :user/pw-salt {:db/doc       "Salt of password"
                         :db/valueType :db.type/bytes}

          :database/name {:db/doc       "Database name, must be unique"
                          :db/unique    :db.unique/identity
                          :db/valueType :db.type/string}
          :database/type {:db/doc       "Database type, :datalog or :key-value"
                          :db/valueType :db.type/keyword}

          :role/key {:db/doc       "Role name, a keyword, must be unique"
                     :db/valueType :db.type/keyword
                     :db/unique    :db.unique/identity}

          :permission/act {:db/doc       "Securable action: ::view, ::alter,
                                          ::create, or ::control"
                           :db/valueType :db.type/keyword}
          :permission/obj {:db/doc       "Securable object type: ::database,
                                          ::user, ::role, or ::server"
                           :db/valueType :db.type/keyword}
          :permission/tgt {:db/doc       "Securable target, an entity id"
                           :db/valueType :db.type/ref}

          :user-role/user {:db/doc       "User part of a user role assignment"
                           :db/valueType :db.type/ref}
          :user-role/role {:db/doc       "Role part of a user role assignment"
                           :db/valueType :db.type/ref}

          :role-perm/role {:db/doc       "Role part of a role permission grant"
                           :db/valueType :db.type/ref}
          :role-perm/perm {:db/doc       "Permission part of a permission grant"
                           :db/valueType :db.type/ref}}))

;; permission securable actions
(derive ::alter ::view)
(derive ::create ::alter)
(derive ::control ::create)

(def permission-actions #{::view ::alter ::create ::control})

;; permission securable object types
(derive ::server ::database)
(derive ::server ::user)
(derive ::server ::role)

(def permission-objects #{::database ::user ::role ::server})

(defn salt
  "generate a 16 byte salt"
  []
  (let [bs (byte-array 16)]
    (.nextBytes (SecureRandom.) bs)
    bs))

(defn password-hashing
  "hashing password using argon2id algorithm, see
  https://github.com/p-h-c/phc-winner-argon2"
  ([password salt]
   (password-hashing password salt nil))
  ([^String password ^bytes salt
    {:keys [ops-limit mem-limit out-length parallelism]
     ;; these defaults are secure, as it takes about 0.5 second to hash
     :or   {ops-limit   4
            mem-limit   131072
            out-length  32
            parallelism 1}}]
   (let [builder (doto (Argon2Parameters$Builder. Argon2Parameters/ARGON2_id)
                   (.withVersion Argon2Parameters/ARGON2_VERSION_13)
                   (.withIterations ops-limit)
                   (.withMemoryAsKB mem-limit)
                   (.withParallelism parallelism)
                   (.withSalt salt))
         gen     (doto (Argon2BytesGenerator.)
                   (.init (.build builder)))
         out-bs  (byte-array out-length)
         in-bs   (.getBytes password StandardCharsets/UTF_8)]
     (.generateBytes gen in-bs out-bs (int 0) (int out-length))
     (b/encode-base64 out-bs))))

(defn password-matches?
  [in-password password-hash salt]
  (= password-hash (password-hashing in-password salt)))

(defn- pull-user
  [sys-conn username]
  {:pre [(d/conn? sys-conn)]}
  (try
    (d/pull @sys-conn '[*] [:user/name username])
    (catch Exception _
      nil)))

(defn- query-user
  [sys-conn username]
  {:pre [(d/conn? sys-conn)]}
  (d/q '[:find ?u .
         :in $ ?uname
         :where
         [?u :user/name ?uname]]
       @sys-conn username))

(defn- pull-db
  [sys-conn db-name]
  {:pre [(d/conn? sys-conn)]}
  (try
    (d/pull @sys-conn '[*] [:database/name db-name])
    (catch Exception _
      nil)))

(defn- query-role
  [sys-conn role-key]
  {:pre [(d/conn? sys-conn)]}
  (d/q '[:find ?r .
         :in $ ?rk
         :where
         [?r :role/key ?rk]]
       @sys-conn role-key))

(defn- user-eid [sys-conn username] (query-user sys-conn username))

(defn- db-eid [sys-conn db-name] (:db/id (pull-db sys-conn db-name)))

(defn- role-eid [sys-conn role-key] (query-role sys-conn role-key))

(defn- eid->username
  [sys-conn eid]
  (:user/name (d/pull @sys-conn [:user/name] eid)))

(defn- eid->db-name
  [sys-conn eid]
  (:database/name (d/pull @sys-conn [:database/name] eid)))

(defn- eid->role-key
  [sys-conn eid]
  (:user/name (d/pull @sys-conn [:user/anme] eid)))

(defn- query-users [sys-conn]
  (d/q '[:find [?uname ...]
         :where
         [?u :user/name ?uname]]
       @sys-conn))

(defn- user-roles
  [sys-conn username]
  (d/q '[:find [?rk ...]
         :in $ ?uname
         :where
         [?u :user/name ?uname]
         [?ur :user-role/user ?u]
         [?ur :user-role/role ?r]
         [?r :role/key ?rk]]
       @sys-conn username))

(defn- query-roles [sys-conn]
  (d/q '[:find [?rk ...]
         :where
         [?r :role/key ?rk]]
       @sys-conn))

(defn- perm-tgt-eid
  [sys-conn perm-obj perm-tgt]
  (case perm-obj
    ::database (db-eid sys-conn perm-tgt)
    ::user     (user-eid sys-conn perm-tgt)
    ::role     (role-eid sys-conn perm-tgt)
    ::server   nil))

(defn- perm-tgt-name
  [sys-conn perm-obj perm-tgt]
  (case perm-obj
    ::database (eid->db-name sys-conn perm-tgt)
    ::user     (eid->username sys-conn perm-tgt)
    ::role     (eid->role-key sys-conn perm-tgt)
    ::server   nil))

(defn- user-permissions
  ([sys-conn username ]
   (mapv first
         (d/q '[:find (pull ?p [:permission/act :permission/obj :permission/tgt])
                :in $ ?uname
                :where
                [?u :user/name ?uname]
                [?ur :user-role/user ?u]
                [?ur :user-role/role ?r]
                [?rp :role-perm/role ?r]
                [?rp :role-perm/perm ?p]]
              @sys-conn username))))

(defn- role-permissions
  [sys-conn role-key]
  (mapv first
        (d/q '[:find (pull ?p [:permission/act :permission/obj :permission/tgt])
               :in $ ?rk
               :where
               [?r :role/key ?rk]
               [?ur :user-role/role ?r]
               [?rp :role-perm/role ?r]
               [?rp :role-perm/perm ?p]]
             @sys-conn role-key)))

(defn- user-role-eid
  [sys-conn uid rid]
  (when (and uid rid)
    (d/q '[:find ?ur .
           :in $ ?u ?r
           :where
           [?ur :user-role/user ?u]
           [?ur :user-role/role ?r]]
         @sys-conn uid rid)))

(defn- permission-eid
  ([sys-conn perm-tgt]
   (when perm-tgt
     (d/q '[:find [?p ...]
            :in $ ?tgt
            :where
            [?p :permission/tgt ?tgt]]
          @sys-conn perm-tgt)))
  ([sys-conn perm-act perm-obj perm-tgt]
   (if perm-tgt
     (d/q '[:find ?p .
            :in $ ?act ?obj ?tgt
            :where
            [?p :permission/act ?act]
            [?p :permission/obj ?obj]
            [?p :permission/tgt ?tgt]]
          @sys-conn perm-act perm-obj perm-tgt)
     (d/q '[:find ?p .
            :in $ ?act ?obj
            :where
            [?p :permission/act ?act]
            [?p :permission/obj ?obj]
            (not [?p :permission/tgt ?tgt])]
          @sys-conn perm-act perm-obj))))

(defn- role-permission-eid
  [sys-conn rid pid]
  (when (and rid pid)
    (d/q '[:find ?rp .
           :in $ ?r ?p
           :where
           [?rp :role-perm/role ?r]
           [?rp :role-perm/perm ?p]]
         @sys-conn rid pid)))

(defn- query-databases
  [sys-conn]
  (d/q '[:find [?dname ...]
         :where
         [?d :database/name ?dname]]
       @sys-conn))

;; each user is a role, similar to postgres
(defn- user-role-key [username] (keyword "datalevin.role" username))

(defn- user-role-key?
  ([sys-conn role-key]
   (user-role-key? sys-conn role-key nil))
  ([sys-conn role-key username]
   (let [ns (namespace role-key)
         n  (name role-key)]
     (and (= ns "datalevin.role")  (query-user sys-conn n)
          (if username (= n username) true)))))

(defn- transact-new-user
  [sys-conn username password]
  (if (query-user sys-conn username)
    (u/raise "User already exits" {:username username})
    (let [s (salt)]
      (d/transact! sys-conn [{:db/id        -1
                              :user/name    username
                              :user/pw-hash (password-hashing password s)
                              :user/pw-salt s}
                             {:db/id    -2
                              :role/key (user-role-key username)}
                             {:db/id          -3
                              :user-role/user -1
                              :user-role/role -2}
                             {:db/id          -4
                              :permission/act ::alter
                              :permission/obj ::user
                              :permission/tgt -1}
                             {:db/id          -5
                              :role-perm/perm -4
                              :role-perm/role -2}
                             {:db/id          -6
                              :permission/act ::view
                              :permission/obj ::role
                              :permission/tgt -2}
                             {:db/id          -7
                              :role-perm/perm -6
                              :role-perm/role -2}]))))

(defn- transact-new-password
  [sys-conn username password]
  (let [s (salt)]
    (d/transact! sys-conn [{:user/name    username
                            :user/pw-hash (password-hashing password s)
                            :user/pw-salt s}])))

(defn- transact-drop-user
  [sys-conn uid username]
  (let [rid    (role-eid sys-conn (user-role-key username))
        urid   (user-role-eid sys-conn uid rid)
        pids   (permission-eid sys-conn uid)
        p-txs  (mapv (fn [pid] [:db/retractEntity pid]) pids)
        rpids  (mapv #(role-permission-eid sys-conn rid %) pids)
        rp-txs (mapv (fn [rpid] [:db/retractEntity rpid]) rpids)]
    (d/transact! sys-conn (u/concatv rp-txs p-txs
                                     [[:db/retractEntity urid]
                                      [:db/retractEntity rid]
                                      [:db/retractEntity uid]]))))

(defn- transact-new-role
  [sys-conn role-key]
  (if (query-role sys-conn role-key)
    (u/raise "Role already exits" {:role-key role-key})
    (d/transact! sys-conn [{:db/id    -1
                            :role/key role-key}
                           {:db/id          -2
                            :permission/act ::view
                            :permission/obj ::role
                            :permission/tgt -1}
                           {:db/id          -3
                            :role-perm/perm -2
                            :role-perm/role -1}])))

(defn- transact-drop-role
  [sys-conn rid]
  (let [ur-txs (mapv (fn [urid] [:db/retractEntity urid])
                     (d/q '[:find [?ur ...]
                            :in $ ?rid
                            :where
                            [?ur :user-role/role ?rid]]
                          @sys-conn rid))
        pids   (permission-eid sys-conn rid)
        p-txs  (mapv (fn [pid] [:db/retractEntity pid]) pids)
        rpids  (mapv #(role-permission-eid sys-conn rid %) pids)
        rp-txs (mapv (fn [rpid] [:db/retractEntity rpid]) rpids)]
    (d/transact! sys-conn (u/concatv rp-txs p-txs ur-txs
                                     [[:db/retractEntity rid]]))))

(defn- transact-user-role
  [sys-conn rid username]
  (if-let [uid (user-eid sys-conn username)]
    (d/transact! sys-conn [{:user-role/user uid :user-role/role rid}])
    (u/raise "User does not exist." {:username username})))

(defn- transact-withdraw-role
  [sys-conn rid username]
  (if-let [uid (user-eid sys-conn username)]
    (when-let [urid (user-role-eid sys-conn uid rid)]
      (d/transact! sys-conn [[:db/retractEntity urid]]))
    (u/raise "User does not exist." {:username username})))

(defn- transact-role-permission
  [sys-conn rid perm-act perm-obj perm-tgt]
  (if perm-tgt
    (if-let [tid (perm-tgt-eid sys-conn perm-obj perm-tgt)]
      (if-let [pid (permission-eid sys-conn perm-act perm-obj tid)]
        (d/transact! sys-conn [{:role-perm/perm pid :role-perm/role rid}])
        (d/transact! sys-conn [{:db/id          -1
                                :permission/act perm-act
                                :permission/obj perm-obj
                                :permission/tgt tid}
                               {:db/id          -2
                                :role-perm/perm -1
                                :role-perm/role rid}]))
      (u/raise "Permission target does not exist." {}))
    (d/transact! sys-conn [{:db/id          -1
                            :permission/act perm-act
                            :permission/obj perm-obj}
                           {:db/id          -2
                            :role-perm/perm -1
                            :role-perm/role rid}])))

(defn- transact-revoke-permission
  [sys-conn rid perm-act perm-obj perm-tgt]
  (if-let [pid (permission-eid sys-conn perm-act perm-obj perm-tgt)]
    (when-let [rpid (role-permission-eid sys-conn rid pid)]
      (d/transact! sys-conn [[:db/retractEntity rpid]]))
    (u/raise "Permission does not exist." {})))

(defn- transact-new-db
  [sys-conn username db-type db-name]
  (d/transact! sys-conn
               [{:db/id         -1
                 :database/type db-type
                 :database/name db-name}
                {:db/id          -2
                 :permission/act ::create
                 :permission/obj ::database
                 :permission/tgt -1}
                {:db/id          -3
                 :role-perm/perm -2
                 :role-perm/role [:role/key (user-role-key username)]}]))

(defn- transact-drop-db
  [sys-conn did]
  (let [pids     (d/q '[:find [?p ...]
                        :in $ ?did
                        :where
                        [?p :permission/tgt ?did]]
                      @sys-conn did)
        pids-txs (mapv (fn [pid] [:db/retractEntity pid]) pids)
        rpids    (mapcat (fn [pid]
                           (d/q '[:find [?rp ...]
                                  :in $ ?pid
                                  :where
                                  [?rp :role-perm/perm ?pid]]
                                @sys-conn pid))
                         pids)
        rp-txs   (mapv (fn [rpid] [:db/retractEntity rpid]) rpids)]
    (d/transact! sys-conn (u/concatv rp-txs pids-txs
                                     [[:db/retractEntity did]]))))

(defn- close-store
  [store]
  (cond
    (instance? IStore store) (i/close store)
    (instance? ILMDB store)  (i/close-kv store)
    :else                    (u/raise "Unknown store" {})))

(defn- has-permission?
  [req-act req-obj req-tgt user-permissions]
  (some (fn [{:keys [permission/act permission/obj permission/tgt] :as p}]
          (and (isa? act req-act)
               (isa? obj req-obj)
               (if req-tgt
                 (if tgt (= req-tgt (tgt :db/id)) true)
                 (if tgt false true))))
        user-permissions))

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
    (.shutdown dispatcher)
    (.shutdown work-executor)
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

(defn- get-stores
  [^Server server]
  (into {} (map (fn [[db-name m]] [db-name (:store m)]) (.-dbs server))))

(defn- get-store
  ([^Server server db-name writing?]
   (get-in (.-dbs server) [db-name (if writing? :wstore :store)]))
  ([server db-name]
   (get-store server db-name false)))

(defn- update-db
  [^Server server db-name f]
  (let [^Map dbs (.-dbs server)
        new      (f (get dbs db-name {}))]
    (.put dbs db-name new)))

(defn- add-store
  [^Server server db-name store]
  (update-db server db-name
             #(cond-> (assoc % :store store)
                (instance? IStore store) (assoc :dt-db (db/new-db store)))))

(defn- get-db
  ([server db-name]
   (get-db server db-name false))
  ([^Server server db-name writing?]
   (let [m (get (.-dbs server) db-name)]
     (if writing? (:wdt-db m) (:dt-db m)))))

(defn- remove-store
  [^Server server db-name]
  (when-let [store (get-store server db-name)]
    (if-let [db (get-db server db-name)]
      (db/close-db db)
      (close-store store)))
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

(defmacro wrap-error
  [& body]
  `(try
     ~@body
     (catch Exception ~'e
       (let [data# (ex-data ~'e)]
         (if (= (:type data#) :reopen)
           (reopen-response ~'skey data#)
           (do (log/error ~'e)
               (error-response ~'skey (ex-message ~'e) (ex-data ~'e))))))))

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
  (or (if writing?
        (writing-lmdb server db-name)
        (db-store server skey db-name))
      (u/raise "LMDB store not found"
               {:type :reopen :db-name db-name :db-type "kv"})))

(defn- store-closed?
  [store]
  (cond
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
      (st/open dir)
      (let [lmdb (l/open-kv dir)]
        (doseq [dbi dbis] (i/open-dbi lmdb dbi))
        lmdb))))

(defn- open-server-store
  "Open a store. NB. stores are left open"
  [^Server server ^SelectionKey skey
   {:keys [db-name schema opts return-db-info?]} db-type]
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
        (let [dir      (db-dir (.-root server) db-name)
              store    (or (when-let [ds (get-store server db-name)]
                             (if (instance? Store ds)
                               (when-not (i/closed? ds)
                                 (when schema
                                   (i/set-schema ds schema))
                                 ds)
                               (when-not (i/closed-kv? ds)
                                 ds)))
                           (case db-type
                             :datalog   (st/open dir schema opts)
                             :key-value (l/open-kv dir opts)))
              datalog? (instance? Store store)]
          (add-store server db-name store)
          (update-client server client-id
                         #(cond-> %
                            true     (update :stores assoc db-name
                                             {:datalog? datalog?
                                              :dbis     #{}})
                            datalog? (update :dt-dbs conj db-name)))
          (when-not existing-db?
            (transact-new-db sys-conn username db-type db-name)
            (update-client server client-id
                           #(assoc % :permissions
                                   (user-permissions sys-conn username))))
          (let [db-info (when (and return-db-info? datalog?)
                          {:max-eid       (i/init-max-eid store)
                           :max-tx        (i/max-tx store)
                           :last-modified (i/last-modified store)
                           :opts          (i/opts store)})]
            (write-message skey
                           (cond-> {:type :command-complete}
                             db-info (assoc :result db-info)))))))))

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
      (.put dbs db-name
            (assoc m :store (open-store root db-name dbis datalog?))))
    (doseq [db-name engines
            :when   (not (get-in dbs [db-name :engine]))
            :let    [m (get dbs db-name {})]]
      (.put dbs db-name
            (assoc m :engine
                   (d/new-search-engine (get-in dbs [db-name :store])))))
    (doseq [db-name indices
            :when   (not (get-in dbs [db-name :index]))
            :let    [m (get dbs db-name {})]]
      (.put dbs db-name
            (assoc m :index
                   (d/new-vector-index (get-in dbs [db-name :store])))))
    (doseq [db-name dt-dbs
            :when   (not (get-in dbs [db-name :dt-db]))
            :let    [m (get dbs db-name {})]]
      (.put dbs db-name
            (assoc m :dt-db
                   (db/new-db (get-in dbs [db-name :store])))))))

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
   'txlog-watermarks
   'open-tx-log
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
    (let [sys-conn            (.-sys-conn server)
          {:keys [client-id]} @(.attachment skey)
          {:keys [username]}  (get-client server client-id)
          [db-name db-type]   args
          db-name             (u/lisp-case db-name)]
      (wrap-permission
        ::create ::database nil
        "Don't have permission to create database"
        (if (db-exists? server db-name)
          (u/raise "Database already exists." {:db db-name})
          (do
            (open-server-store server skey
                               {:db-name db-name} db-type)
            (transact-new-db sys-conn username db-type db-name)
            (update-client server client-id
                           #(assoc % :permissions
                                   (user-permissions sys-conn username)))))
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
  (wrap-error (normal-dt-store-handler close)))

(defn- closed?
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          res     (if-let [s (store server skey db-name writing?)]
                    (i/closed? s)
                    true)]
      (write-message skey {:type :command-complete :result res}))))

(defn- opts
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler opts)))

(defn- assoc-opt
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler assoc-opt)))

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
        (let [^DB new-db (db/new-db (get-store server db-name writing?))]
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
    (update-db server (nth args 0) #(dissoc % :wlmdb :lock))
    (normal-kv-store-handler close-kv)))

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

(defn- copy
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [[db-name compact?] args
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
          (i/copy (lmdb server skey db-name writing?) tf compact?))
        (let [completed-ms (System/currentTimeMillis)
              copy-meta (cond-> {:started-ms started-ms
                                 :completed-ms completed-ms
                                 :duration-ms (- completed-ms started-ms)
                                 :compact? (boolean compact?)}
                          (map? @copy-backup-pin)
                          (assoc :backup-pin @copy-backup-pin))]
          (copy-file-out skey path copy-meta))
        (finally (u/delete-files tf))))))

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

(defn- get-kv-store
  [server db-name]
  (let [s (get-store server db-name)]
    (if (instance? Store s) (.-lmdb ^Store s) s)))

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
        (let [kv-store (get-kv-store server db-name)]
          (locking kv-store
            (update-db server db-name
                       #(assoc % :wlmdb (i/open-transact-kv kv-store)))
            (let [runner (write-txn-runner server db-name kv-store)]
              (write-message skey {:type :command-complete})
              (run-calls runner))))))))

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
        (i/close-transact-kv kv-store)
        (halt-run (get-in dbs [db-name :runner]))
        (update-db server db-name #(dissoc % :runner :wlmdb))
        (write-message skey {:type :command-complete})
        (.release ^Semaphore (get-in dbs [db-name :lock]))))))

(defn- abort-transact-kv
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (normal-kv-store-handler abort-transact-kv)))))

(defn- open-transact
  [^Server server ^SelectionKey skey {:keys [args] :as message}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (.acquire ^Semaphore (get-lock server db-name))
        (let [kv-store (get-kv-store server db-name)]
          (locking kv-store
            (let [wlmdb  (i/open-transact-kv kv-store)
                  ostore (get-store server db-name)
                  wstore (st/transfer ostore wlmdb)
                  runner (write-txn-runner server db-name kv-store)]
              (update-db
                server db-name #(assoc %
                                       :wlmdb wlmdb
                                       :wstore wstore
                                       :wdt-db (db/new-db wstore)))
              (write-message skey {:type :command-complete})
              (run-calls runner))))))))

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
        (i/close-transact-kv kv-store)
        (halt-run (get-in dbs [db-name :runner]))
        (add-store
          server db-name
          (st/transfer (get-in dbs [db-name :wstore]) kv-store))
        (update-db
          server db-name
          #(dissoc % :wlmdb :wstore :wdt-db :runner))
        (write-message skey {:type :command-complete})
        (.release ^Semaphore (get-in dbs [db-name :lock]))))))

(defn- abort-transact
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)]
      (wrap-permission
          ::alter ::database (db-eid sys-conn db-name)
          "Don't have permission to alter the database"
        (i/abort-transact-kv kv-store)
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
        (let [{:keys [type] :as message} @msg
              skey                       @sk]
          (message-cases skey type))
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

(defn- handle-writing
  [^Server server ^SelectionKey skey {:keys [args] :as message}]
  (try
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          runner   (get-in (.-dbs server) [db-name :runner])]
      (new-message runner skey message)
      (locking kv-store (.notify kv-store)))
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
          {:keys [type writing?] :as message}
          (p/read-value fmt msg wire-opts)]
      (log/debug "Message received:" (dissoc message :password :args))
      (set-last-active server skey)
      (if writing?
        (handle-writing server skey message)
        (message-cases skey type)))
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
    :or   {port         8898
           root         "/var/lib/datalevin"
           idle-timeout c/default-idle-timeout
           verbose      false}}]
  {:pre [(int? port) (not (s/blank? root))]}
  (try
    (log/set-min-level! (if verbose :debug :info))
    (let [^ServerSocketChannel server-socket (open-port port)
          ^Selector selector                 (Selector/open)
          running                            (AtomicBoolean. false)
          sys-conn                           (init-sys-db root (get-default-password))
          clients                            (load-sessions sys-conn)
          dbs                                (ConcurrentHashMap.)]
      (reopen-dbs root clients dbs)
      (.register server-socket selector SelectionKey/OP_ACCEPT)
      (->Server running
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
                dbs))
    (catch Exception e
      (u/raise "Error creating server:" (ex-message e) {}))))
