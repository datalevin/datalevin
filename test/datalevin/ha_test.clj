(ns datalevin.ha-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.ha :as ha]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.util :as hu]))

(deftest saturated-long-add-clamps-positive-overflow
  (is (= Long/MAX_VALUE
         (hu/saturated-long-add (- Long/MAX_VALUE 5) 10))))

(deftest lease-expiry-clamps-large-deadlines
  (is (not (#'datalevin.ha/ha-lease-expired-for-promotion?
            {:ha-clock-skew-budget-ms 10}
            {:lease-until-ms (- Long/MAX_VALUE 5)}
            0)))
  (is (#'datalevin.ha/ha-lease-expired-for-promotion?
       {:ha-clock-skew-budget-ms 10}
       {:lease-until-ms (- Long/MAX_VALUE 5)}
       Long/MAX_VALUE)))

(deftest fencing-failure-releases-authoritative-lease-before-demotion
  (let [authority (ctrl/new-in-memory-authority
                   {:group-id (str (java.util.UUID/randomUUID))
                    :scope-id (Object.)})
        db-name "db"
        db-identity "db-id"]
    (try
      (ctrl/start-authority! authority)
      (let [acquire (ctrl/try-acquire-lease
                     authority
                     {:db-identity db-identity
                      :leader-node-id 1
                      :leader-endpoint "dtlv://leader"
                      :lease-renew-ms 1000
                      :lease-timeout-ms 15000
                      :leader-last-applied-lsn 0
                      :now-ms 1
                      :observed-version 0
                      :observed-lease nil})
            lease (:lease acquire)
            state {:ha-role :leader
                   :ha-node-id 1
                   :ha-db-identity db-identity
                   :ha-authority authority
                   :ha-authority-term (:term lease)
                   :ha-authority-lease lease
                   :ha-leader-term (:term lease)
                   :ha-leader-fencing-pending? true
                   :ha-leader-fencing-observed-lease nil
                   :ha-demotion-drain-ms 1}]
        (with-redefs [ha/run-ha-fencing-hook
                      (fn [_ _ _]
                        {:ok? false
                         :reason :test-failure})]
          (let [next-state (#'datalevin.ha/maybe-complete-ha-leader-fencing
                            state
                            db-name)
                snapshot (ctrl/read-state authority db-identity)]
            (is (= :demoting (:ha-role next-state)))
            (is (= :fencing-incomplete
                   (:ha-demotion-reason next-state)))
            (is (= true
                   (get-in next-state
                           [:ha-demotion-details :lease-release :released?])))
            (is (nil? (:lease snapshot))))))
      (finally
        (ctrl/stop-authority! authority)))))
