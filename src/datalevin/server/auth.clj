;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server.auth
  "System database schema, authentication, and RBAC helpers for the server."
  (:require
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.util :as u])
  (:import
   [java.nio.charset StandardCharsets]
   [java.security SecureRandom]
   [org.bouncycastle.crypto.generators Argon2BytesGenerator]
   [org.bouncycastle.crypto.params Argon2Parameters
    Argon2Parameters$Builder]))

(def ^:private view-act :datalevin.server/view)
(def ^:private alter-act :datalevin.server/alter)
(def ^:private create-act :datalevin.server/create)
(def ^:private control-act :datalevin.server/control)

(def ^:private database-obj :datalevin.server/database)
(def ^:private user-obj :datalevin.server/user)
(def ^:private role-obj :datalevin.server/role)
(def ^:private server-obj :datalevin.server/server)

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

(derive alter-act view-act)
(derive create-act alter-act)
(derive control-act create-act)

(derive server-obj database-obj)
(derive server-obj user-obj)
(derive server-obj role-obj)

(def permission-actions #{view-act alter-act create-act control-act})

(def permission-objects #{database-obj user-obj role-obj server-obj})

(defn salt
  "Generate a 16 byte salt."
  []
  (let [bs (byte-array 16)]
    (.nextBytes (SecureRandom.) bs)
    bs))

(defn password-hashing
  "Hash a password using argon2id."
  ([password salt]
   (password-hashing password salt nil))
  ([^String password ^bytes salt
    {:keys [ops-limit mem-limit out-length parallelism]
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

(defn pull-user
  [sys-conn username]
  {:pre [(d/conn? sys-conn)]}
  (try
    (d/pull @sys-conn '[*] [:user/name username])
    (catch Exception _
      nil)))

(defn query-user
  [sys-conn username]
  {:pre [(d/conn? sys-conn)]}
  (d/q '[:find ?u .
         :in $ ?uname
         :where
         [?u :user/name ?uname]]
       @sys-conn username))

(defn pull-db
  [sys-conn db-name]
  {:pre [(d/conn? sys-conn)]}
  (try
    (d/pull @sys-conn '[*] [:database/name db-name])
    (catch Exception _
      nil)))

(defn query-role
  [sys-conn role-key]
  {:pre [(d/conn? sys-conn)]}
  (d/q '[:find ?r .
         :in $ ?rk
         :where
         [?r :role/key ?rk]]
       @sys-conn role-key))

(defn user-eid [sys-conn username] (query-user sys-conn username))

(defn db-eid [sys-conn db-name] (:db/id (pull-db sys-conn db-name)))

(defn role-eid [sys-conn role-key] (query-role sys-conn role-key))

(defn eid->username
  [sys-conn eid]
  (:user/name (d/pull @sys-conn [:user/name] eid)))

(defn eid->db-name
  [sys-conn eid]
  (:database/name (d/pull @sys-conn [:database/name] eid)))

(defn eid->role-key
  [sys-conn eid]
  (:role/key (d/pull @sys-conn [:role/key] eid)))

(defn query-users
  [sys-conn]
  (d/q '[:find [?uname ...]
         :where
         [?u :user/name ?uname]]
       @sys-conn))

(defn user-roles
  [sys-conn username]
  (d/q '[:find [?rk ...]
         :in $ ?uname
         :where
         [?u :user/name ?uname]
         [?ur :user-role/user ?u]
         [?ur :user-role/role ?r]
         [?r :role/key ?rk]]
       @sys-conn username))

(defn query-roles
  [sys-conn]
  (d/q '[:find [?rk ...]
         :where
         [?r :role/key ?rk]]
       @sys-conn))

(defn perm-tgt-eid
  [sys-conn perm-obj perm-tgt]
  (case perm-obj
    :datalevin.server/database (db-eid sys-conn perm-tgt)
    :datalevin.server/user     (user-eid sys-conn perm-tgt)
    :datalevin.server/role     (role-eid sys-conn perm-tgt)
    :datalevin.server/server   nil))

(defn perm-tgt-name
  [sys-conn perm-obj perm-tgt]
  (case perm-obj
    :datalevin.server/database (eid->db-name sys-conn perm-tgt)
    :datalevin.server/user     (eid->username sys-conn perm-tgt)
    :datalevin.server/role     (eid->role-key sys-conn perm-tgt)
    :datalevin.server/server   nil))

(defn user-permissions
  [sys-conn username]
  (mapv first
        (d/q '[:find (pull ?p [:permission/act :permission/obj :permission/tgt])
               :in $ ?uname
               :where
               [?u :user/name ?uname]
               [?ur :user-role/user ?u]
               [?ur :user-role/role ?r]
               [?rp :role-perm/role ?r]
               [?rp :role-perm/perm ?p]]
             @sys-conn username)))

(defn role-permissions
  [sys-conn role-key]
  (mapv first
        (d/q '[:find (pull ?p [:permission/act :permission/obj :permission/tgt])
               :in $ ?rk
               :where
               [?r :role/key ?rk]
               [?rp :role-perm/role ?r]
               [?rp :role-perm/perm ?p]]
             @sys-conn role-key)))

(defn user-role-eid
  [sys-conn uid rid]
  (when (and uid rid)
    (d/q '[:find ?ur .
           :in $ ?u ?r
           :where
           [?ur :user-role/user ?u]
           [?ur :user-role/role ?r]]
         @sys-conn uid rid)))

(defn permission-eid
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

(defn role-permission-eid
  [sys-conn rid pid]
  (when (and rid pid)
    (d/q '[:find ?rp .
           :in $ ?r ?p
           :where
           [?rp :role-perm/role ?r]
           [?rp :role-perm/perm ?p]]
         @sys-conn rid pid)))

(defn query-databases
  [sys-conn]
  (d/q '[:find [?dname ...]
         :where
         [?d :database/name ?dname]]
       @sys-conn))

(defn user-role-key
  [username]
  (keyword "datalevin.role" username))

(defn user-role-key?
  ([sys-conn role-key]
   (user-role-key? sys-conn role-key nil))
  ([sys-conn role-key username]
   (let [ns (namespace role-key)
         n  (name role-key)]
     (and (= ns "datalevin.role")
          (query-user sys-conn n)
          (if username (= n username) true)))))

(defn transact-new-user
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
                              :permission/act alter-act
                              :permission/obj user-obj
                              :permission/tgt -1}
                             {:db/id          -5
                              :role-perm/perm -4
                              :role-perm/role -2}
                             {:db/id          -6
                              :permission/act view-act
                              :permission/obj role-obj
                              :permission/tgt -2}
                             {:db/id          -7
                              :role-perm/perm -6
                              :role-perm/role -2}]))))

(defn transact-new-password
  [sys-conn username password]
  (let [s (salt)]
    (d/transact! sys-conn [{:user/name    username
                            :user/pw-hash (password-hashing password s)
                            :user/pw-salt s}])))

(defn transact-drop-user
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

(defn transact-new-role
  [sys-conn role-key]
  (if (query-role sys-conn role-key)
    (u/raise "Role already exits" {:role-key role-key})
    (d/transact! sys-conn [{:db/id    -1
                            :role/key role-key}
                           {:db/id          -2
                            :permission/act view-act
                            :permission/obj role-obj
                            :permission/tgt -1}
                           {:db/id          -3
                            :role-perm/perm -2
                            :role-perm/role -1}])))

(defn transact-drop-role
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

(defn transact-user-role
  [sys-conn rid username]
  (if-let [uid (user-eid sys-conn username)]
    (d/transact! sys-conn [{:user-role/user uid :user-role/role rid}])
    (u/raise "User does not exist." {:username username})))

(defn transact-withdraw-role
  [sys-conn rid username]
  (if-let [uid (user-eid sys-conn username)]
    (when-let [urid (user-role-eid sys-conn uid rid)]
      (d/transact! sys-conn [[:db/retractEntity urid]]))
    (u/raise "User does not exist." {:username username})))

(defn transact-role-permission
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

(defn transact-revoke-permission
  [sys-conn rid perm-act perm-obj perm-tgt]
  (if perm-tgt
    (if-let [tid (perm-tgt-eid sys-conn perm-obj perm-tgt)]
      (if-let [pid (permission-eid sys-conn perm-act perm-obj tid)]
        (when-let [rpid (role-permission-eid sys-conn rid pid)]
          (d/transact! sys-conn [[:db/retractEntity rpid]]))
        (u/raise "Permission does not exist." {}))
      (u/raise "Permission target does not exist." {}))
    (if-let [pid (permission-eid sys-conn perm-act perm-obj nil)]
      (when-let [rpid (role-permission-eid sys-conn rid pid)]
        (d/transact! sys-conn [[:db/retractEntity rpid]]))
      (u/raise "Permission does not exist." {}))))

(defn transact-new-db
  [sys-conn username db-type db-name]
  (d/transact! sys-conn
               [{:db/id         -1
                 :database/type db-type
                 :database/name db-name}
                {:db/id          -2
                 :permission/act create-act
                 :permission/obj database-obj
                 :permission/tgt -1}
                {:db/id          -3
                 :role-perm/perm -2
                 :role-perm/role [:role/key (user-role-key username)]}]))

(defn transact-drop-db
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

(defn has-permission?
  [req-act req-obj req-tgt user-permissions]
  (some (fn [{:keys [permission/act permission/obj permission/tgt]}]
          (and (isa? act req-act)
               (isa? obj req-obj)
               (if req-tgt
                 (if tgt (= req-tgt (tgt :db/id)) true)
                 (if tgt false true))))
        user-permissions))
