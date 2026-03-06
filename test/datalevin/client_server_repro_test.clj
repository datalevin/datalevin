(ns datalevin.client-server-repro-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.server :as srv]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(defn- with-server
  [f]
  (let [dir    (u/tmp-dir (str "client-server-repro-" (UUID/randomUUID)))
        server (binding [c/*db-background-sampling?* false]
                 (srv/create {:port c/default-port
                              :root dir}))]
    (try
      (srv/start server)
      (f server)
      (finally
        (srv/stop server)
        (u/delete-files dir)
        (System/gc)))))

(defn- local-role-perms
  [server role-key]
  (vec (sort-by pr-str (#'srv/role-permissions (.-sys-conn server) role-key))))

(defn- op-seq
  [server client db-name role-key]
  (let [before-roles (vec (sort (cl/list-roles client)))
        create-db    (cl/create-database client db-name :datalog)
        create-role  (cl/create-role client role-key)
        after-create (vec (sort (cl/list-roles client)))
        create-perms (vec (sort-by pr-str
                                   (cl/list-role-permissions client role-key)))
        local-create (local-role-perms server role-key)
        grant        (cl/grant-permission client role-key
                                          :datalevin.server/alter
                                          :datalevin.server/database
                                          db-name)
        after-grant  (vec (sort-by pr-str
                                   (cl/list-role-permissions client role-key)))
        local-grant  (local-role-perms server role-key)
        revoke       (try
                       (cl/revoke-permission client role-key
                                             :datalevin.server/alter
                                             :datalevin.server/database
                                             db-name)
                       (catch Exception e
                         {:exception (ex-message e)
                          :data      (ex-data e)}))
        after-revoke (vec (sort-by pr-str
                                   (cl/list-role-permissions client role-key)))
        local-revoke (local-role-perms server role-key)]
    {:before-roles before-roles
     :create-db create-db
     :create-role create-role
     :after-create after-create
     :after-create-perms create-perms
     :local-after-create local-create
     :grant grant
     :after-grant after-grant
     :local-after-grant local-grant
     :revoke revoke
     :after-revoke after-revoke
     :local-after-revoke local-revoke}))

(defn- cleanup!
  [client db-name role-key]
  (try
    (cl/close-database client db-name)
    (catch Exception _))
  (try
    (cl/drop-database client db-name)
    (catch Exception _))
  (try
    (cl/drop-role client role-key)
    (catch Exception _)))

(defn- validate-run
  [{:keys [after-create after-create-perms after-grant after-revoke
           local-after-create
           local-after-grant local-after-revoke] :as result}
   role-key]
  (is (some #{role-key} after-create) (pr-str result))
  (is (= local-after-create after-create-perms)
      (pr-str result))
  (is (= local-after-grant after-grant)
      (pr-str result))
  (is (= local-after-revoke after-revoke)
      (pr-str result))
  (is (= 1 (count local-after-create))
      (pr-str result))
  (is (= 2 (count local-after-grant))
      (pr-str result))
  (is (= 1 (count local-after-revoke))
      (pr-str result)))

(defn run-repro!
  ([] (run-repro! 20))
  ([iterations]
   (doseq [pool-size [1 2]]
     (with-server
       (fn [server]
         (dotimes [i iterations]
           (let [suffix   (str (UUID/randomUUID))
                 db-name  (str "client-server-repro-db-" suffix)
                 role-key (keyword (str "client-server-repro-role-" suffix))
                 client   (cl/new-client "dtlv://datalevin:datalevin@localhost"
                                         {:pool-size pool-size})]
             (try
               (let [result (op-seq server client db-name role-key)]
                 (when-not (and (some #{role-key} (:after-create result))
                                (= (:local-after-create result)
                                   (:after-create-perms result))
                                (= (:local-after-grant result)
                                   (:after-grant result))
                                (= (:local-after-revoke result)
                                   (:after-revoke result))
                                (= 1 (count (:local-after-create result)))
                                (= 2 (count (:local-after-grant result)))
                                (= 1 (count (:local-after-revoke result))))
                   (throw (ex-info "Remote admin sequence mismatch"
                                   {:pool-size pool-size
                                    :iteration i
                                    :db-name db-name
                                    :role-key role-key
                                    :result result}))))
               (finally
                 (cleanup! client db-name role-key)
                 (cl/disconnect client))))))))))

(defn raw-seq!
  [server]
  (let [suffix   (str (UUID/randomUUID))
        db-name  (str "client-server-repro-db-" suffix)
        role-key (keyword (str "client-server-repro-role-" suffix))
        client   (cl/new-client "dtlv://datalevin:datalevin@localhost"
                                {:pool-size 1})
        pool     (cl/get-pool client)
        conn     (cl/get-connection pool)
        reqs     [{:label :list-roles
                   :msg   {:type :list-roles :args [] :writing? false}}
                  {:label :create-db
                   :msg   {:type :create-database
                           :args [db-name :datalog]
                           :writing? false}}
                  {:label :create-role
                   :msg   {:type :create-role
                           :args [role-key]
                           :writing? false}}
                  {:label :list-role-perms-1
                   :msg   {:type :list-role-permissions
                           :args [role-key]
                           :writing? false}}
                  {:label :grant
                   :msg   {:type :grant-permission
                           :args [role-key
                                  :datalevin.server/alter
                                  :datalevin.server/database
                                  db-name]
                           :writing? false}}
                  {:label :list-role-perms-2
                   :msg   {:type :list-role-permissions
                           :args [role-key]
                           :writing? false}}
                  {:label :revoke
                   :msg   {:type :revoke-permission
                           :args [role-key
                                  :datalevin.server/alter
                                  :datalevin.server/database
                                  db-name]
                           :writing? false}}
                  {:label :list-role-perms-3
                   :msg   {:type :list-role-permissions
                           :args [role-key]
                           :writing? false}}]]
    (try
      (reduce
        (fn [acc {:keys [label msg]}]
          (assoc acc label {:response (cl/send-n-receive conn msg)
                            :local    (local-role-perms server role-key)}))
        {:db-name db-name
         :role-key role-key}
        reqs)
      (finally
        (cl/release-connection pool conn)
        (cleanup! client db-name role-key)
        (cl/disconnect client)))))

(defn run-raw-repro!
  []
  (with-server raw-seq!))

(deftest remote-admin-sequence-test
  (doseq [pool-size [1 2]]
    (testing (str "pool-size=" pool-size)
      (with-server
        (fn [server]
          (let [suffix   (str (UUID/randomUUID))
                db-name  (str "client-server-repro-db-" suffix)
                role-key (keyword (str "client-server-repro-role-" suffix))
                client   (cl/new-client "dtlv://datalevin:datalevin@localhost"
                                        {:pool-size pool-size})]
            (try
              (let [result (op-seq server client db-name role-key)]
                (validate-run result role-key))
              (finally
                (cleanup! client db-name role-key)
                (cl/disconnect client)))))))))

(defn -main
  [& [iterations]]
  (if (= iterations "raw")
    (prn (run-raw-repro!))
    (run-repro! (Long/parseLong (or iterations "20")))))
