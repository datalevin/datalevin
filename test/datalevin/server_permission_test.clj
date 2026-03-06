;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.server-permission-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.server :as srv]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(defn- permission-match?
  [perm act obj]
  (and (= act (:permission/act perm))
       (= obj (:permission/obj perm))))

(deftest revoke-permission-targeted-database-test
  (let [dir      (u/tmp-dir (str "server-permission-" (UUID/randomUUID)))
        server   (srv/create {:port 0 :root dir})
        sys-conn (.-sys-conn server)
        db-name  (str "server-permission-db-" (UUID/randomUUID))
        role-key (keyword (str "server-permission-role-" (UUID/randomUUID)))]
    (try
      (#'srv/transact-new-db sys-conn "datalevin" :datalog db-name)
      (#'srv/transact-new-role sys-conn role-key)
      (let [rid (#'srv/role-eid sys-conn role-key)]
        (#'srv/transact-role-permission
          sys-conn rid
          :datalevin.server/alter
          :datalevin.server/database
          db-name)
        (let [perms1 (#'srv/role-permissions sys-conn role-key)]
          (is (= 2 (count perms1)))
          (is (some #(permission-match? %
                                        :datalevin.server/view
                                        :datalevin.server/role)
                    perms1))
          (is (some #(permission-match? %
                                        :datalevin.server/alter
                                        :datalevin.server/database)
                    perms1))
          (#'srv/transact-revoke-permission
            sys-conn rid
            :datalevin.server/alter
            :datalevin.server/database
            db-name)
          (let [perms2 (#'srv/role-permissions sys-conn role-key)]
            (is (= 1 (count perms2)))
            (is (some #(permission-match? %
                                          :datalevin.server/view
                                          :datalevin.server/role)
                      perms2))
            (is (not-any? #(permission-match? %
                                               :datalevin.server/alter
                                               :datalevin.server/database)
                          perms2)))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))

(deftest client-display-shows-role-target-key-test
  (let [dir        (u/tmp-dir (str "server-permission-" (UUID/randomUUID)))
        server     (srv/create {:port 0 :root dir})
        sys-conn   (.-sys-conn server)
        role-key   (keyword (str "server-permission-role-" (UUID/randomUUID)))
        client-id  (UUID/randomUUID)]
    (try
      (#'srv/transact-new-role sys-conn role-key)
      (let [display (second (#'srv/client-display
                              server
                              [client-id {:ip          "127.0.0.1"
                                          :username    "datalevin"
                                          :roles       [role-key]
                                          :permissions (#'srv/role-permissions
                                                         sys-conn role-key)
                                          :stores      {}}]))]
        (is (some #{[:datalevin.server/view
                     :datalevin.server/role
                     role-key]}
                  (:permissions display))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))
