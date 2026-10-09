(ns datalevin.server.auth-test
  "Guards the single sources for the permission taxonomy and the privileged
  server option keys against drift."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.server.auth :as auth]))

(deftest permission-taxonomy-equality-test
  (testing "securable actions"
    (is (= #{:datalevin.server/view
             :datalevin.server/alter
             :datalevin.server/create
             :datalevin.server/control}
           auth/permission-actions)))
  (testing "securable objects"
    (is (= #{:datalevin.server/database
             :datalevin.server/user
             :datalevin.server/role
             :datalevin.server/server}
           auth/permission-objects))))

(deftest privileged-server-option-keys-equality-test
  (is (= #{:ha-mode
           :ha-control-plane
           :ha-members
           :ha-membership-hash
           :ha-node-id
           :ha-client-credentials
           :ha-fencing-hook
           :ha-clock-skew-hook
           :runtime-opts
           :snapshot-dir
           :spill-opts
           :embedding-opts
           :embedding-domains
           :embedding-providers
           :embedding-domain-providers}
         auth/privileged-server-option-keys)
      "the server option filter and the message handlers must share this set")
  (testing "the handlers predicate agrees with the single source"
    (let [privileged? (requiring-resolve
                       'datalevin.server.handlers/privileged-server-option-key?)]
      (doseq [k auth/privileged-server-option-keys]
        (is (true? (privileged? k)) (str "expected privileged: " k)))
      (is (false? (privileged? :not-a-privileged-option))))))
