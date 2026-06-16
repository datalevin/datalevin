(ns datalevin.test.ha-control
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [datalevin.ha.authority :as authority]
   [datalevin.ha.control :as ctrl]
   [datalevin.server.ha :as server-ha]
   [datalevin.util :as u]))

(def apply-state-command #'ctrl/apply-state-command)

(deftest membership-hash-update-cas-test
  (let [state {:leases {"app" {:lease {:db-identity "app"
                                        :leader-node-id 1
                                        :leader-endpoint "dtlv://n1/app"
                                        :term 1
                                        :lease-until-ms 1000}
                               :version 1}}
               :membership-hash nil
               :voters ["127.0.0.1:9001"]}
        init  (apply-state-command state
                                   {:op :init-membership-hash
                                    :membership-hash "old"})
        wrong (apply-state-command (:state init)
                                   {:op :update-membership-hash
                                    :req {:expected-membership-hash "stale"
                                          :membership-hash "new"}})
        updated (apply-state-command (:state init)
                                     {:op :update-membership-hash
                                      :req {:expected-membership-hash "old"
                                            :membership-hash "new"}})
        retried (apply-state-command (:state updated)
                                     {:op :update-membership-hash
                                      :req {:expected-membership-hash "old"
                                            :membership-hash "new"}})]
    (is (= {:ok? true
            :initialized? true
            :membership-hash "old"}
           (:result init)))
    (is (= {:ok? false
            :reason :membership-hash-mismatch
            :membership-hash "old"
            :expected "stale"}
           (:result wrong)))
    (is (= (:state init) (:state wrong)))
    (is (= "new" (get-in updated [:state :membership-hash])))
    (is (= {} (get-in updated [:state :leases])))
    (is (= {:ok? true
            :updated? true
            :membership-hash "new"
            :previous-membership-hash "old"
            :cleared-leases 1}
           (:result updated)))
    (is (= {:ok? true
            :updated? false
            :idempotent? true
            :membership-hash "new"
            :previous-membership-hash "new"
            :cleared-leases 0}
           (:result retried)))
    (is (= (:state updated) (:state retried)))))

(deftest authority-read-freshness-uses-renew-window-test
  (let [m {:ha-authority-read-ok? true
           :ha-last-authority-refresh-ms 1000
           :ha-lease-renew-ms 1000
           :ha-lease-timeout-ms 3000
           :ha-write-admission-lease-margin-ms 100}]
    (is (= 2000 (authority/ha-authority-read-fresh-timeout-ms m)))
    (is (authority/ha-authority-read-fresh? m 2999))
    (is (not (authority/ha-authority-read-fresh? m 3000)))
    (is (= {:reason :authority-read-stale
            :last-authority-refresh-ms 1000
            :timeout-ms 2000
            :lease-renew-ms 1000
            :lease-timeout-ms 3000
            :write-admission-margin-ms 100}
           (authority/ha-authority-read-failure-details m 3000)))))

(deftest server-ha-control-raft-dir-default-is-under-db-root-test
  (let [root    "/var/lib/datalevin"
        db-name "orders"
        ha-opts {:ha-control-plane
                 {:backend :sofa-jraft
                  :group-id "ha/prod"
                  :local-peer-id "10.0.0.12:7801"}}
        resolved (server-ha/with-default-ha-control-raft-dir
                  root db-name ha-opts)
        raft-dir (get-in resolved [:ha-control-plane :raft-dir])]
    (is (str/starts-with?
         raft-dir
         (str root u/+separator+ "ha-control" u/+separator+)))
    (is (str/includes? raft-dir (u/hexify-string db-name)))))
