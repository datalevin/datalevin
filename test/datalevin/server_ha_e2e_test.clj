(ns datalevin.server-ha-e2e-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.db :as db]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ha]
   [datalevin.interpret :as interp]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.server :as srv]
   [datalevin.server-ha-test-support :refer :all]
   [datalevin.storage :as st]
   [datalevin.udf :as udf]
   [datalevin.util :as u]
   [datalevin.validate :as vld]
   [taoensso.timbre :as log])
  (:import
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.server Server]
   [java.net ConnectException]
   [java.nio.channels ClosedChannelException]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue]
   [java.util.concurrent.atomic AtomicBoolean]))

(use-fixtures :once quiet-server-ha-logs-fixture)

(deftest ha-e2e-in-memory-three-node-failover-test
  (let [result (run-e2e-ha-failover! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (string? (:initial-leader-endpoint result)))
    (is (string? (:new-leader-endpoint result)))
    (is (not= (:initial-leader-endpoint result)
              (:new-leader-endpoint result)))))

(deftest ha-e2e-sofa-jraft-three-node-failover-characterization-test
  (when (= "1" (System/getenv "DTLV_RUN_HA_E2E_SOFA_JRAFT"))
    (let [result (run-e2e-ha-failover! :sofa-jraft)]
      (is (= :sofa-jraft (:control-backend result)))
      (is (string? (:initial-leader-endpoint result)))
      (is (string? (:new-leader-endpoint result)))
      (is (not= (:initial-leader-endpoint result)
                (:new-leader-endpoint result))))))

(deftest ha-e2e-in-memory-three-node-follower-rejoin-test
  (let [result (run-e2e-ha-follower-rejoin! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (= (:initial-leader-id result)
           (:rejoined-node-id result)))
    (is (not= (:current-leader-id result)
              (:rejoined-node-id result)))
    (is (= :follower (get-in result [:rejoin-state :ha-role])))
    (is (= (:current-leader-id result)
           (get-in result [:rejoin-state :ha-authority-owner-node-id])))
    (is (integer? (get-in result [:rejoin-state :ha-authority-term])))
    (is (>= (long (or (get-in result [:replica-floor :replica :floor-lsn]) 0))
            (long (or (get-in result
                              [:rejoin-state :last-applied-lsn])
                      0))))))

(deftest ha-e2e-in-memory-three-node-rejoin-bootstrap-test
  (let [result (run-e2e-ha-rejoin-bootstrap! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (= (:initial-leader-id result)
           (:rejoined-node-id result)))
    (is (not= (:current-leader-id result)
              (:rejoined-node-id result)))
    (is (= :follower (get-in result [:bootstrap-state :ha-role])))
    (is (= (:current-leader-id result)
           (get-in result [:bootstrap-state :ha-authority-owner-node-id])))
    (is (integer? (get-in result [:bootstrap-state :ha-follower-last-bootstrap-ms])))
    (is (pos? (long (or (get-in result
                                [:bootstrap-state
                                 :ha-follower-bootstrap-snapshot-last-applied-lsn])
                        0))))
    (is (contains? (:source-endpoints result)
                   (get-in result
                           [:bootstrap-state
                            :ha-follower-bootstrap-source-endpoint])))
    (is (= :follower (get-in result [:rejoin-state :ha-role])))
    (is (nil? (get-in result [:rejoin-state :ha-follower-degraded?])))
    (is (nil? (get-in result [:rejoin-state :ha-follower-last-error])))
    (is (>= (long (or (get-in result [:replica-floor :replica :floor-lsn]) 0))
            (long (or (get-in result
                              [:rejoin-state :last-applied-lsn])
                      0))))))

(deftest ha-e2e-in-memory-three-node-membership-hash-drift-recovery-test
  (let [result (run-e2e-ha-membership-hash-drift! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (integer? (:drifted-node-id result)))
    (is (= :ha/membership-hash-mismatch
           (get-in result [:drift-error :data :err-data :error])))
    (is (integer? (:recovered-leader-id result)))
    (is (true? (get-in result
                       [:recovered-node-diagnostics
                        (:drifted-node-id result)
                        :ha-authority-diagnostics
                        :running?])))))

(deftest ha-e2e-in-memory-three-node-fencing-hook-verify-test
  (let [result (run-e2e-ha-fencing-hook-verify! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (string? (:initial-leader-endpoint result)))
    (is (string? (:new-leader-endpoint result)))
    (is (not= (:initial-leader-id result)
              (:new-leader-id result)))
    (is (= (str (:initial-leader-id result))
           (get-in result [:verified-entry :old-node-id])))
    (is (= (:initial-leader-endpoint result)
           (get-in result [:verified-entry :old-leader-endpoint])))
    (is (= (str (:new-leader-id result))
           (get-in result [:verified-entry :new-node-id])))
    (is (= (str "ha-e2e:"
                (get-in result [:verified-entry :observed-term])
                ":"
                (:new-leader-id result))
           (get-in result [:verified-entry :fence-op-id])))))

(deftest ha-e2e-in-memory-three-node-clock-skew-pause-test
  (let [result (run-e2e-ha-clock-skew-pause! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (= 2 (count (:paused-node-ids result))))
    (is (= (:resume-node-id result)
           (:resumed-leader-id result)))
    (is (not= (:resume-node-id result)
              (:blocked-node-id result)))
    (is (every? true?
                (map #(true? (get-in result
                                     [:blocked-result :paused-states %
                                      :ha-clock-skew-paused?]))
                     (:paused-node-ids result))))
    (is (every? #(= :clock-skew-budget-breached
                    (get-in result
                            [:blocked-result :paused-states %
                             :ha-promotion-failure-details :reason]))
                (:paused-node-ids result)))
    (is (= :leader (get-in result [:leader-state :ha-role])))
    (is (false? (get-in result [:leader-state :ha-clock-skew-paused?])))
    (is (= :follower
           (get-in result [:resumed-follower-state :ha-role])))
    (is (false? (get-in result
                        [:resumed-follower-state :ha-clock-skew-paused?])))))

(deftest ha-e2e-in-memory-three-node-degraded-mode-no-valid-source-test
  (let [result (run-e2e-ha-degraded-mode-no-valid-source! :in-memory)]
    (is (= :in-memory (:control-backend result)))
    (is (integer? (:degraded-node-id result)))
    (is (true? (get-in result [:degraded-state :ha-follower-degraded?])))
    (is (= :wal-gap
           (get-in result [:degraded-state :ha-follower-degraded-reason])))
    (is (true? (get-in result [:blocked-state :ha-follower-degraded?])))
    (is (= (:initial-leader-id result)
           (get-in result [:blocked-state :ha-authority-owner-node-id])))
    (is (nil? (get-in result [:recovered-state :ha-follower-degraded?])))
    (is (nil? (get-in result [:recovered-state :ha-follower-last-error])))))

(deftest ha-e2e-sofa-jraft-three-node-follower-rejoin-characterization-test
  (when (= "1" (System/getenv "DTLV_RUN_HA_E2E_SOFA_JRAFT"))
    (let [result (run-e2e-ha-follower-rejoin! :sofa-jraft)]
      (is (= :sofa-jraft (:control-backend result)))
      (is (= (:initial-leader-id result)
             (:rejoined-node-id result)))
      (is (not= (:current-leader-id result)
                (:rejoined-node-id result)))
      (is (= :follower (get-in result [:rejoin-state :ha-role]))))))

(deftest ha-e2e-sofa-jraft-three-node-rejoin-bootstrap-characterization-test
  (when (= "1" (System/getenv "DTLV_RUN_HA_E2E_SOFA_JRAFT"))
    (let [result (run-e2e-ha-rejoin-bootstrap! :sofa-jraft)]
      (is (= :sofa-jraft (:control-backend result)))
      (is (= (:initial-leader-id result)
             (:rejoined-node-id result)))
      (is (not= (:current-leader-id result)
                (:rejoined-node-id result)))
      (is (= :follower (get-in result [:bootstrap-state :ha-role])))
      (is (integer? (get-in result
                            [:bootstrap-state :ha-follower-last-bootstrap-ms]))))))
