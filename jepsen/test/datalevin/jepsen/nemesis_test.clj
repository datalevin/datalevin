(ns datalevin.jepsen.nemesis-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.nemesis :as nemesis]
   [jepsen.nemesis :as jn]))

(deftest restore-quorum-nodes-retries-pending-nodes-test
  (testing "restore-quorum retries nodes that fail a prior restart round"
    (let [attempts (atom {})
          result   (with-redefs [local/workload-setup-timeout-ms
                                 (fn
                                   ([_cluster-id]
                                    1000)
                                   ([_cluster-id _default-timeout-ms]
                                    1000))
                                 local/restart-node!
                                 (fn [_cluster-id logical-node]
                                   (let [attempt (get (swap! attempts
                                                             update
                                                             logical-node
                                                             (fnil inc 0))
                                                      logical-node)]
                                     (when (and (= "n2" logical-node)
                                                (= 1 attempt))
                                       (throw (ex-info "retry me"
                                                       {:logical-node logical-node
                                                        :attempt attempt})))
                                     true))]
                     (#'nemesis/restore-quorum-nodes! :cluster
                                                      ["n1" "n2"]))]
      (is (= {:restarted ["n1" "n2"]
              :pending []
              :errors {}}
             result))
      (is (= {"n1" 1
              "n2" 2}
             @attempts)))))

(deftest restore-quorum-nodes-retains-pending-nodes-on-timeout-test
  (testing "restore-quorum leaves still-failed nodes pending for a later retry"
    (let [attempts (atom {})
          result   (with-redefs [local/workload-setup-timeout-ms
                                 (fn
                                   ([_cluster-id]
                                    1)
                                   ([_cluster-id _default-timeout-ms]
                                    1))
                                 local/restart-node!
                                 (fn [_cluster-id logical-node]
                                   (swap! attempts update logical-node (fnil inc 0))
                                   (throw (ex-info "still down"
                                                   {:logical-node logical-node})))]
                     (#'nemesis/restore-quorum-nodes! :cluster
                                                      ["n2"]))]
      (is (= [] (:restarted result)))
      (is (= ["n2"] (:pending result)))
      (is (= "still down"
             (get-in result [:errors "n2" :message])))
      (is (pos? (get @attempts "n2" 0))))))

(deftest clock-skew-final-phase-stabilizes-leader-test
  (testing "clock skew cleanup waits for write admission to recover"
    (is (= [{:type :info :f :clear-clock-skew}
            {:type :info :f :stabilize-leader}]
           (vec (#'nemesis/final-phase-ops
                 {:clock-skew? true})))))
  (testing "clock skew plus failover keeps the existing restart step"
    (is (= [{:type :info :f :clear-clock-skew}
            {:type :info :f :restart-node}
            {:type :info :f :stabilize-leader}]
           (vec (#'nemesis/final-phase-ops
                 {:clock-skew? true
                  :failover? true}))))))

(deftest clock-skew-final-stabilize-is-hard-bounded-test
  (testing "stabilize-leader returns when the leader probe blocks past its deadline"
    (let [started? (promise)
          nemesis  (:nemesis (nemesis/nemesis-package
                              {:faults [:clock-skew-mixed]}))
          result   (with-redefs-fn
                     {#'datalevin.jepsen.nemesis/default-final-leader-stabilize-timeout-ms
                      50
                      #'datalevin.jepsen.local/maybe-wait-for-single-leader
                      (fn [_cluster-id _timeout-ms]
                        (deliver started? true)
                        (Thread/sleep 10000)
                        {:leader "n1"})}
                     (fn []
                       (let [started-ms (System/currentTimeMillis)
                             op         (jn/invoke!
                                         nemesis
                                         {:datalevin/cluster-id :cluster}
                                         {:type :info
                                          :process :nemesis
                                          :f :stabilize-leader})
                             elapsed-ms (- (System/currentTimeMillis)
                                           started-ms)]
                         {:op op
                          :elapsed-ms elapsed-ms})))]
      (is (deref started? 500 false))
      (is (= {:type :info
              :process :nemesis
              :f :stabilize-leader
              :value {:leader nil
                      :status :leader-timeout}}
             (:op result)))
      (is (< (:elapsed-ms result) 1000)))))
