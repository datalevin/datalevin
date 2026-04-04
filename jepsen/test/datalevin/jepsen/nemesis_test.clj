(ns datalevin.jepsen.nemesis-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.nemesis :as nemesis]))

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
